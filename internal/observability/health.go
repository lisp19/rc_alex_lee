package observability

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/redis/go-redis/v9"
	"log/slog"
	"net/http"
	"notifier/internal/application"
	"notifier/internal/config"
	"notifier/internal/delivery"
	"notifier/internal/mq/rabbitmq"
	"notifier/internal/quota"
	"notifier/internal/recovery"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

type Health struct {
	Business, Control *sql.DB
	Redis             *redis.Client
	MQ                *rabbitmq.Client
	Config            *config.Manager
	Roles             []string
	Draining          atomic.Bool
	API               *application.Service
	Worker            *delivery.Worker
	Recovery          *recovery.Loop
	Quota             *quota.Limiter
}

func (h *Health) dependencies(ctx context.Context) (bool, map[string]any) {
	detail := map[string]any{"draining": h.Draining.Load(), "roles": h.Roles}
	instance, _ := os.Hostname()
	detail["instance_id"] = instance
	var dbOK, controlOK, redisOK bool
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); dbOK = h.Business.PingContext(ctx) == nil }()
	go func() { defer wg.Done(); controlOK = h.Control.PingContext(ctx) == nil }()
	go func() { defer wg.Done(); redisOK = h.Redis.Ping(ctx).Err() == nil }()
	wg.Wait()
	mqOK := h.MQ.Ready()
	detail["business_db"] = dbOK
	detail["control_db"] = controlOK
	detail["redis"] = redisOK
	detail["rabbitmq"] = mqOK
	snap := h.Config.Current()
	configOK := snap != nil
	ready := dbOK && configOK && !h.Draining.Load()
	if slices.Contains(h.Roles, "worker") || slices.Contains(h.Roles, "outbox") {
		ready = ready && mqOK
	}
	if snap != nil {
		detail["config_revision"] = snap.Revision
		if !redisOK {
			for _, q := range snap.Quotas {
				if q.FailMode == "closed" {
					ready = false
				}
			}
		}
	}
	detail["worker_active"] = h.MQ.Active.Load()
	detail["quota_degraded"] = h.Quota.Degraded.Load() || !redisOK
	detail["ready"] = ready
	return ready, detail
}
func (h *Health) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) { write(w, 200, map[string]bool{"live": true}) })
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		ok, _ := h.dependencies(ctx)
		status := 503
		if ok {
			status = 200
		}
		write(w, status, map[string]bool{"ready": ok})
	})
	mux.HandleFunc("GET /health/detail", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		_, detail := h.dependencies(ctx)
		write(w, 200, detail)
	})
	return mux
}
func write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Debug("health_write_failed")
	}
}
func (h *Health) Stats(ctx context.Context) {
	tick := time.NewTicker(30 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			c, cancel := context.WithTimeout(ctx, 3*time.Second)
			var pending, inflight, outbox, age int64
			err := h.Business.QueryRowContext(c, `SELECT COALESCE(SUM(status='pending'),0),COALESCE(SUM(status='in_flight'),0),COALESCE(TIMESTAMPDIFF(SECOND,MIN(IF(status='pending',created_at,NULL)),UTC_TIMESTAMP(6)),0) FROM notification_task WHERE status IN ('pending','in_flight')`).Scan(&pending, &inflight, &age)
			if err == nil {
				err = h.Business.QueryRowContext(c, "SELECT COUNT(*) FROM mq_outbox WHERE status IN ('pending','publishing')").Scan(&outbox)
			}
			cancel()
			if err != nil {
				slog.Warn("backlog_stats_unavailable")
			}
			rev := uint64(0)
			if s := h.Config.Current(); s != nil {
				rev = s.Revision
			}
			slog.Info("periodic_stats", "accepted_total", h.API.Accepted.Load(), "delivered_total", h.Worker.Delivered.Load(), "failed_total", h.Worker.Failed.Load(), "retry_scheduled_total", h.Worker.Retried.Load(), "pending_count", pending, "in_flight_count", inflight, "oldest_pending_age_seconds", age, "outbox_pending_count", outbox, "mq_publish_failed_total", h.MQ.PublishFailures.Load(), "mq_consume_failed_total", h.MQ.ConsumeFailures.Load(), "lease_recovered_total", h.Recovery.Recovered.Load(), "quota_rejected_total", h.Quota.Rejected.Load(), "config_reload_total", h.Config.Reloads.Load(), "config_reload_failed_total", h.Config.Failures.Load(), "config_revision", rev)
		}
	}
}
