package rabbitmq

import (
	"net"
	"time"
)

// PublishWithContext does not interrupt blocked writes in amqp091-go.
// Per-write deadlines also bound confirm waits and shutdown during failures.
type boundedConn struct{ net.Conn }

func (c boundedConn) Write(b []byte) (int, error) {
	if err := c.Conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return 0, err
	}
	return c.Conn.Write(b)
}
