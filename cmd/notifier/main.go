package main

import (
	"context"
	"errors"
	"github.com/redis/go-redis/v9"
	"log/slog"
	"net"
	"net/http"
	"notifier/internal/api"
	"notifier/internal/application"
	"notifier/internal/config"
	"notifier/internal/delivery"
	"notifier/internal/hook"
	"notifier/internal/lock"
	"notifier/internal/mq/rabbitmq"
	"notifier/internal/observability"
	"notifier/internal/outbox"
	"notifier/internal/quota"
	"notifier/internal/recovery"
	"notifier/internal/repository"
	"notifier/internal/repository/business"
	"notifier/internal/retry"
	"notifier/internal/target"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("notifier_stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	path := os.Getenv("NOTIFIER_CONFIG")
	if path == "" {
		path = "configs/notifier.example.json"
	}
	p, err := config.LoadProcess(path)
	if err != nil {
		return err
	}
	bdb, err := repository.Open(p.BusinessDSN, p.DBMaxOpen)
	if err != nil {
		return errors.New("invalid business database configuration")
	}
	defer bdb.Close()
	cdb, err := repository.Open(p.ControlDSN, max(4, p.DBMaxOpen/4))
	if err != nil {
		return errors.New("invalid control database configuration")
	}
	defer cdb.Close()
	opts, err := redis.ParseURL(p.RedisURL)
	if err != nil {
		return errors.New("invalid Redis URL")
	}
	opts.DialTimeout = time.Second
	opts.ReadTimeout = 500 * time.Millisecond
	opts.WriteTimeout = 500 * time.Millisecond
	opts.MaxRetries = -1
	opts.ContextTimeoutEnabled = true
	rdb := redis.NewClient(opts)
	defer rdb.Close()
	manager := config.NewManager(cdb)
	if _, err = manager.LoadSecrets(p.Secrets); err != nil {
		return err
	}
	startup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	if err = bdb.PingContext(startup); err == nil {
		err = manager.Reload(startup)
	}
	cancel()
	if err != nil {
		return errors.New("initial database/configuration load failed")
	}
	mq := &rabbitmq.Client{URL: p.AMQPURL}
	defer mq.Close()
	store := &business.Store{DB: bdb}
	adapter := target.NewHTTP()
	defer adapter.Close()
	limiter := quota.New(rdb, p.Workers, p.TargetConcurrency)
	worker := &delivery.Worker{Store: store, Config: manager, Adapter: adapter, Gate: limiter, Lock: &lock.Redis{Client: rdb}, Decide: hook.Evaluate, Schedule: retry.Schedule}
	service := &application.Service{Store: store, Config: manager, Quota: limiter, Proxy: worker.Process}
	service.ManualRetry = service.Retry
	recoverer := &recovery.Loop{Store: store, Config: manager}
	health := &observability.Health{Business: bdb, Control: cdb, Redis: rdb, MQ: mq, Config: manager, Roles: p.Roles, API: service, Worker: worker, Recovery: recoverer, Quota: limiter}
	stop, stopSignal := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stopSignal()
	attemptCtx, abortAttempts := context.WithCancel(context.Background())
	defer abortAttempts()
	loops, stopLoops := context.WithCancel(context.Background())
	defer stopLoops()
	var wg sync.WaitGroup
	launch := func(fn func()) { wg.Add(1); go func() { defer wg.Done(); fn() }() }
	launch(func() { manager.Watch(loops, path, p) })
	launch(func() { health.Stats(loops) })
	if slices.Contains(p.Roles, "worker") || slices.Contains(p.Roles, "outbox") {
		launch(func() {
			tick := time.NewTicker(2 * time.Second)
			defer tick.Stop()
			for {
				if !mq.Ready() {
					if err := mq.Ensure(); err != nil {
						slog.Warn("mq_connection_unavailable")
					}
				}
				select {
				case <-loops.Done():
					return
				case <-tick.C:
				}
			}
		})
	}
	if slices.Contains(p.Roles, "worker") {
		launch(func() { mq.Consume(stop, attemptCtx, p.Workers, p.Prefetch, worker.Process) })
	}
	if slices.Contains(p.Roles, "outbox") {
		dispatcher := &outbox.Dispatcher{Store: store, MQ: mq}
		launch(func() { dispatcher.Run(loops) })
	}
	if slices.Contains(p.Roles, "recovery") {
		launch(func() { recoverer.Run(loops) })
	}
	serverErrors := make(chan error, 2)
	startServer := func(addr string, handler http.Handler) (*http.Server, error) {
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			return nil, err
		}
		s := &http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 45 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32768, BaseContext: func(net.Listener) context.Context { return attemptCtx }}
		go func() {
			if err := s.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				serverErrors <- err
			}
		}()
		return s, nil
	}
	hs, err := startServer(p.HealthListen, health.Handler())
	if err != nil {
		return err
	}
	defer hs.Close()
	var apiServer *http.Server
	if slices.Contains(p.Roles, "api") {
		apiServer, err = startServer(p.Listen, (&api.Server{Service: service}).Handler())
		if err != nil {
			return err
		}
		defer apiServer.Close()
	}
	slog.Info("notifier_started", "roles", p.Roles, "config_revision", manager.Current().Revision)
	select {
	case <-stop.Done():
	case err = <-serverErrors:
		stopSignal()
	}
	health.Draining.Store(true)
	stopSignal()
	stopLoops()
	grace, graceCancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer graceCancel()
	apiDone := make(chan struct{})
	go func() {
		defer close(apiDone)
		if apiServer != nil {
			if err := apiServer.Shutdown(grace); err != nil {
				slog.Warn("api_drain_timeout")
			}
		}
	}()
	done := make(chan struct{})
	go func() { wg.Wait(); <-apiDone; close(done) }()
	select {
	case <-done:
	case <-grace.Done():
		slog.Warn("shutdown_grace_expired")
		abortAttempts()
		mq.Close()
		if apiServer != nil {
			apiServer.Close()
		}
	}
	abortAttempts()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
	defer shutdownCancel()
	if err := hs.Shutdown(shutdownCtx); err != nil {
		slog.Debug("health_shutdown_failed")
	}
	slog.Info("notifier_drained")
	return err
}
