package outbox

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"notifier/internal/mq/rabbitmq"
	"notifier/internal/repository/business"
	"time"
)

type Dispatcher struct {
	Store *business.Store
	MQ    *rabbitmq.Client
}

func (d *Dispatcher) Run(ctx context.Context) {
	for ctx.Err() == nil {
		c, cancel := context.WithTimeout(ctx, 10*time.Second)
		o, err := d.Store.ClaimOutbox(c)
		if err == nil {
			err = d.MQ.Publish(c, o)
			if err == nil {
				err = d.Store.Published(c, o)
			} else {
				d.MQ.PublishFailures.Add(1)
				if e := d.Store.ReleaseOutbox(c, o); e != nil {
					slog.Warn("outbox_release_deferred", "outbox_id", o.ID)
				}
			}
		}
		cancel()
		if err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				slog.Error("outbox_dispatch_failed")
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
		}
	}
}
