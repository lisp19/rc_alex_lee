package recovery

import (
	"context"
	"errors"
	"log/slog"
	"notifier/internal/config"
	"notifier/internal/domain"
	"notifier/internal/repository/business"
	"notifier/internal/retry"
	"sync/atomic"
	"time"
)

type Loop struct {
	Store      *business.Store
	Config     *config.Manager
	Recovered  atomic.Uint64
	Reconciled atomic.Uint64
}

func (l *Loop) Run(ctx context.Context) {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		c, cancel := context.WithTimeout(ctx, 15*time.Second)
		err := l.step(c)
		cancel()
		if err != nil && ctx.Err() == nil {
			slog.Error("recovery_failed")
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
func (l *Loop) step(ctx context.Context) error {
	if err := l.Store.RecoverOutbox(ctx); err != nil {
		return err
	}
	ns, err := l.Store.Expired(ctx)
	if err != nil {
		return err
	}
	for _, n := range ns {
		snap, err := l.Config.History(ctx, n.ConfigRevision)
		if err != nil {
			return err
		}
		t, ok := snap.Targets[n.Target]
		if !ok {
			return errors.New("historical target missing")
		}
		d := domain.Decision{Action: "retry", Reason: "lease_expired", Uncertain: true}
		next, route := retry.Schedule(n, t, snap.Retries[t.RetryPolicy], &d)
		if err = l.Store.RecoverLease(ctx, n, d, next, route); errors.Is(err, domain.ErrState) {
			continue
		} else if err != nil {
			return err
		}
		l.Recovered.Add(1)
		slog.Warn("lease_recovered", "notification_id", n.ID, "generation", n.Generation, "result", d.Action, "error_code", d.Reason)
	}
	count, err := l.Store.Reconcile(ctx)
	if err == nil {
		l.Reconciled.Add(uint64(count))
	}
	return err
}
