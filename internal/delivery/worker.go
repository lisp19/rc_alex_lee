package delivery

import (
	"context"
	"errors"
	"log/slog"
	"notifier/internal/application"
	"notifier/internal/config"
	"notifier/internal/domain"
	"notifier/internal/repository/business"
	"notifier/internal/target"
	"sync/atomic"
	"time"
)

type Gate interface {
	Acquire(context.Context, *domain.Notification, config.Quota) (func(), bool, error)
}
type Locker interface {
	Acquire(context.Context, domain.ID) (func(), bool, error)
}
type Worker struct {
	Store     *business.Store
	Config    *config.Manager
	Adapter   target.Adapter
	Gate      Gate
	Lock      Locker
	Decide    func(context.Context, *domain.Notification, *config.Snapshot, *domain.Decision)
	Schedule  func(*domain.Notification, config.Target, config.Retry, *domain.Decision) (time.Time, string)
	Delivered atomic.Uint64
	Failed    atomic.Uint64
	Retried   atomic.Uint64
}

func (w *Worker) Process(ctx context.Context, id domain.ID, generation uint64) error {
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	n, err := w.Store.Load(ctx, id)
	if errors.Is(err, domain.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if n.Generation != generation || n.Status != "pending" {
		return nil
	}
	if time.Now().Before(n.Next) {
		return nil
	} // Reconciler repairs early/missing triggers.
	historical, err := w.Config.History(ctx, n.ConfigRevision)
	if err != nil {
		return err
	}
	t, ok := historical.Targets[n.Target]
	if !ok {
		return errors.New("historical target missing")
	}
	latest := w.Config.Current()
	if latest == nil {
		return errors.New("configuration unavailable")
	}
	current, policyErr := application.Allowed(latest, n.Client, n.Target)
	if w.Lock != nil {
		release, ok, err := w.Lock.Acquire(ctx, id)
		if err != nil {
			slog.Warn("lock_degraded", "notification_id", id)
		} else if !ok {
			return nil
		} else {
			defer release()
		}
	}
	if w.Gate != nil && policyErr == nil {
		release, ok, err := w.Gate.Acquire(ctx, n, latest.Quotas[current.Quota])
		if err != nil {
			return w.Store.DeferPending(ctx, n, 5*time.Second)
		}
		if !ok {
			return w.Store.DeferPending(ctx, n, 5*time.Second)
		}
		defer release()
	}
	n, err = w.Store.Claim(ctx, id, generation)
	if errors.Is(err, domain.ErrState) {
		return nil
	}
	if err != nil {
		return err
	}
	d := domain.Decision{Action: "fail", Reason: "target_policy_blocked", Started: time.Now().UTC()}
	secret := ""
	if policyErr == nil {
		if current.Auth.Type == "static_header" {
			secret, err = w.Config.Secret(current.Auth.SecretRef)
		}
		if err != nil {
			d.Reason = "secret_unavailable"
		} else if n.CycleAttempts > 1 && !time.Now().Before(n.Deadline) {
			d.Reason = "retry_deadline_exceeded"
		} else {
			d = w.Adapter.Deliver(ctx, n, t, current, secret)
			if w.Decide != nil {
				w.Decide(ctx, n, historical, &d)
			}
		}
	}
	target.Redact(&d, t, secret)
	var next time.Time
	route := "dispatch"
	if d.Action == "retry" {
		if w.Schedule != nil {
			next, route = w.Schedule(n, t, historical.Retries[t.RetryPolicy], &d)
		} else {
			d.Action = "fail"
			d.Reason = "retry_unavailable"
		}
	}
	// Persist independently of caller cancellation. This does not extend HTTP
	// execution; the database lease remains the final authority.
	commitCtx, commitCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer commitCancel()
	err = w.Store.Finish(commitCtx, n, d, next, route)
	if errors.Is(err, domain.ErrState) {
		slog.Warn("stale_attempt_result", "notification_id", id, "generation", generation)
		return nil
	}
	if err != nil {
		return err
	}
	switch d.Action {
	case "success":
		w.Delivered.Add(1)
	case "retry":
		w.Retried.Add(1)
	default:
		w.Failed.Add(1)
	}
	slog.Info("delivery_finished", "notification_id", id, "client_id", n.Client, "target_id", n.Target, "attempt_no", n.Attempts, "generation", generation, "config_revision", historical.Revision, "result", d.Action, "http_status", d.HTTPStatus, "error_code", d.Reason, "latency_ms", d.Latency.Milliseconds())
	return nil
}
