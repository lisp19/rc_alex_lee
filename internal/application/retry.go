package application

import (
	"context"
	"notifier/internal/config"
	"notifier/internal/domain"
)

func (s *Service) Retry(ctx context.Context, client string, id domain.ID, snap *config.Snapshot) error {
	if !snap.Clients[client].ManualRetry {
		return Fail("FORBIDDEN", 403, "manual retry permission required")
	}
	n, err := s.Store.Get(ctx, id, client)
	if err != nil {
		return err
	}
	if _, err = Allowed(snap, client, n.Target); err != nil {
		return err
	}
	if n.Status != "failed" {
		return domain.ErrState
	}
	if s.Quota != nil {
		ok, err := s.Quota.Ingress(ctx, client, snap.Quotas[snap.Clients[client].Quota])
		if err != nil {
			return Fail("DEPENDENCY_UNAVAILABLE", 503, "quota unavailable")
		}
		if !ok {
			return Fail("QUOTA_EXCEEDED", 429, "ingress quota exceeded")
		}
	}
	history, err := s.Config.History(ctx, n.ConfigRevision)
	if err != nil {
		return err
	}
	t, ok := history.Targets[n.Target]
	if !ok {
		return Fail("DEPENDENCY_UNAVAILABLE", 503, "historical config missing")
	}
	return s.Store.ManualRetry(ctx, n, history.Retries[t.RetryPolicy].MaxDuration.Duration())
}
