package retry

import (
	"net/http"
	"notifier/internal/config"
	"notifier/internal/domain"
	"strconv"
	"strings"
	"time"
)

// Schedule maps every retry to a fixed TTL bucket. next_attempt_at is a
// database not-before guard; publisher delays may make delivery later, never
// earlier. Unsupported Retry-After delays terminate rather than violate it.
func Schedule(n *domain.Notification, t config.Target, p config.Retry, d *domain.Decision) (time.Time, string) {
	fail := func(reason string) (time.Time, string) { d.Action = "fail"; d.Reason = reason; return time.Time{}, "" }
	if !t.RetryEnabled {
		return fail("automatic_retry_disabled")
	}
	if d.Uncertain && !UncertainAllowed(t) {
		return fail("uncertain_retry_disabled")
	}
	i := n.CycleAttempts - 1
	if i < 0 || i >= len(p.Delays) {
		return fail("retry_exhausted")
	}
	now := time.Now().UTC()
	delay := p.Delays[i].Duration()
	if p.RespectRetryAfter && d.RetryAfter != "" {
		raw := strings.TrimSpace(d.RetryAfter)
		if seconds, err := strconv.ParseInt(raw, 10, 64); err == nil {
			if seconds > int64((30*24*time.Hour)/time.Second) {
				return fail("retry_after_exceeds_limit")
			}
			if seconds > 0 {
				delay = max(delay, time.Duration(seconds)*time.Second)
			}
		} else if at, err := http.ParseTime(raw); err == nil {
			delay = max(delay, at.Sub(now))
		}
	}
	var bucket time.Duration
	for _, b := range config.Buckets {
		if b >= delay {
			bucket = b
			break
		}
	}
	if bucket == 0 {
		return fail("retry_after_exceeds_buckets")
	}
	next := now.Add(bucket)
	if !next.Before(n.Deadline) {
		return fail("retry_deadline_exceeded")
	}
	return next, config.BucketRoute(bucket)
}
func UncertainAllowed(t config.Target) bool {
	if t.Idempotency.UncertainRetry != nil {
		return *t.Idempotency.UncertainRetry
	}
	return t.Idempotency.Mode == "supported"
}
