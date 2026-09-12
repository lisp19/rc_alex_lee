package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"notifier/internal/config"
	"notifier/internal/domain"
)

type Client struct {
	URL             string
	mu              sync.Mutex
	conn            *amqp.Connection
	observed        atomic.Pointer[amqp.Connection]
	Closed          bool
	PublishFailures atomic.Uint64
	ConsumeFailures atomic.Uint64
	Active          atomic.Int64
}

func (c *Client) connection() (*amqp.Connection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Closed {
		return nil, errors.New("MQ closed")
	}
	if c.conn == nil || c.conn.IsClosed() {
		conn, err := amqp.DialConfig(c.URL, amqp.Config{Heartbeat: 10 * time.Second, Locale: "en_US", Dial: func(network, addr string) (net.Conn, error) {
			conn, err := net.DialTimeout(network, addr, 5*time.Second)
			if err != nil {
				return nil, err
			}
			if err = conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				conn.Close()
				return nil, err
			}
			return boundedConn{conn}, nil
		}})
		if err != nil {
			return nil, errors.New("AMQP connect failed")
		}
		timeout := time.AfterFunc(10*time.Second, func() { _ = conn.CloseDeadline(time.Now()) })
		defer timeout.Stop()
		ch, err := conn.Channel()
		if err != nil {
			conn.Close()
			return nil, err
		}
		if err = topology(ch); err != nil {
			ch.Close()
			conn.Close()
			return nil, err
		}
		ch.Close()
		c.conn = conn
		c.observed.Store(conn)
	}
	return c.conn, nil
}
func (c *Client) channel() (*amqp.Channel, error) {
	conn, err := c.connection()
	if err != nil {
		return nil, err
	}
	timeout := time.AfterFunc(10*time.Second, func() { _ = conn.CloseDeadline(time.Now()) })
	defer timeout.Stop()
	return conn.Channel()
}
func topology(ch *amqp.Channel) error {
	for _, x := range []string{"notify.dispatch.x", "notify.retry.x", "notify.dead.x"} {
		if err := ch.ExchangeDeclare(x, "direct", true, false, false, false, nil); err != nil {
			return err
		}
	}
	queues := []struct {
		name, exchange, route string
		args                  amqp.Table
	}{
		{"notify.dead.q", "notify.dead.x", "dead", amqp.Table{"x-queue-type": "quorum"}},
		{"notify.dispatch.q", "notify.dispatch.x", "dispatch", amqp.Table{"x-queue-type": "quorum", "x-dead-letter-exchange": "notify.dead.x", "x-dead-letter-routing-key": "dead", "x-delivery-limit": int32(-1)}},
	}
	for _, d := range config.Buckets {
		route := config.BucketRoute(d)
		queues = append(queues, struct {
			name, exchange, route string
			args                  amqp.Table
		}{"notify." + route + ".q", "notify.retry.x", route, amqp.Table{"x-queue-type": "quorum", "x-message-ttl": int64(d / time.Millisecond), "x-dead-letter-exchange": "notify.dispatch.x", "x-dead-letter-routing-key": "dispatch", "x-dead-letter-strategy": "at-least-once", "x-overflow": "reject-publish"}})
	}
	for _, q := range queues {
		if _, err := ch.QueueDeclare(q.name, true, false, false, false, q.args); err != nil {
			return err
		}
		if err := ch.QueueBind(q.name, q.route, q.exchange, false, nil); err != nil {
			return err
		}
	}
	return nil
}
func (c *Client) Ready() bool {
	conn := c.observed.Load()
	return conn != nil && !conn.IsClosed()
}
func (c *Client) Ensure() error {
	ch, err := c.channel()
	if err != nil {
		return err
	}
	return ch.Close()
}
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Closed = true
	c.observed.Store(nil)
	if c.conn != nil {
		if err := c.conn.CloseDeadline(time.Now().Add(time.Second)); err != nil {
			slog.Debug("mq_close_failed")
		}
	}
}
func (c *Client) Publish(ctx context.Context, o *domain.Outbox) error {
	ch, err := c.channel()
	if err != nil {
		return err
	}
	conn := c.observed.Load()
	stopTimeout := context.AfterFunc(ctx, func() {
		if conn != nil {
			_ = conn.CloseDeadline(time.Now())
		}
	})
	defer stopTimeout()
	defer ch.Close()
	if err = ch.Confirm(false); err != nil {
		return err
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	returns := ch.NotifyReturn(make(chan amqp.Return, 1))
	body, err := json.Marshal(o.Event)
	if err != nil {
		return err
	}
	exchange := "notify.dispatch.x"
	if o.Route != "dispatch" {
		exchange = "notify.retry.x"
	}
	if err = ch.PublishWithContext(ctx, exchange, o.Route, true, false, amqp.Publishing{ContentType: "application/json", DeliveryMode: amqp.Persistent, MessageId: o.Event.ID.String(), Timestamp: o.Event.Created, Body: body}); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-returns:
		return errors.New("unroutable publish")
	case confirm, ok := <-confirms:
		if !ok || !confirm.Ack {
			return errors.New("publish not confirmed")
		}
		// AMQP sends basic.return before the corresponding confirm. The client
		// dispatches both into buffered channels in protocol order.
		select {
		case <-returns:
			return errors.New("unroutable publish")
		default:
			return nil
		}
	}
}

type Processor func(context.Context, domain.ID, uint64) error

func (c *Client) Consume(ctx, attemptCtx context.Context, workers, prefetch int, process Processor) {
	for ctx.Err() == nil {
		if err := c.consumeSession(ctx, attemptCtx, workers, prefetch, process); err != nil && ctx.Err() == nil {
			c.ConsumeFailures.Add(1)
			slog.Error("mq_consume_failed")
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}
func (c *Client) consumeSession(ctx, attemptCtx context.Context, workers, prefetch int, process Processor) error {
	ch, err := c.channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	if err = ch.Qos(prefetch, 0, false); err != nil {
		return err
	}
	tag := domain.NewID().String()
	messages, err := ch.Consume("notify.dispatch.q", tag, false, false, false, false, nil)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case msg, ok := <-messages:
					if !ok {
						return
					}
					if ctx.Err() != nil {
						return
					}
					var e domain.Event
					if len(msg.Body) > 4096 || json.Unmarshal(msg.Body, &e) != nil || e.Schema != 1 || e.Type != "dispatch" || e.Generation == 0 || e.Notification == (domain.ID{}) || e.ID == (domain.ID{}) {
						c.ConsumeFailures.Add(1)
						slog.Error("invalid_internal_message")
						if msg.Reject(false) != nil {
							return
						}
						continue
					}
					c.Active.Add(1)
					err := process(attemptCtx, e.Notification, e.Generation)
					c.Active.Add(-1)
					if err != nil {
						c.ConsumeFailures.Add(1)
						if msg.Nack(false, true) != nil {
							return
						}
						select {
						case <-ctx.Done():
							return
						case <-time.After(time.Second):
						}
					} else if msg.Ack(false) != nil {
						return
					}
				}
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-ctx.Done():
		if err := ch.Cancel(tag, false); err != nil {
			slog.Debug("consumer_cancel_failed")
		}
		<-done
		return nil
	case <-done:
		return errors.New("consumer channel ended")
	}
}
