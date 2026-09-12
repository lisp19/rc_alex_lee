// Mock targets are only for the later integration acceptance phase.
package main

import (
	"encoding/json"
	"flag"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"
)

func main() {
	addr := flag.String("listen", ":18080", "listen address")
	flag.Parse()
	var mu sync.Mutex
	counts := map[string]int{}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Idempotency-Key")
		if key == "" {
			key = r.Header.Get("X-Notification-ID")
		}
		mu.Lock()
		if len(counts) > 100000 {
			counts = map[string]int{}
		}
		counts[key]++
		count := counts[key]
		mu.Unlock()
		q := r.URL.Query()
		failures, _ := strconv.Atoi(q.Get("failures"))
		if q.Get("reset") == "true" {
			if h, ok := w.(http.Hijacker); ok {
				conn, _, err := h.Hijack()
				if err == nil {
					conn.Close()
				}
			}
			return
		}
		if seconds, _ := strconv.Atoi(q.Get("sleep")); seconds > 0 {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(time.Duration(min(seconds, 60)) * time.Second):
			}
		}
		w.Header().Set("Content-Type", "application/json")
		status := 200
		code := 0
		switch r.URL.Path {
		case "/a":
			if count <= failures {
				status = 500
			}
		case "/b":
			if count <= failures {
				code = 1001
			}
			if q.Get("terminal") == "true" {
				code = 2000
			}
		case "/c":
		case "/d":
			if count <= failures {
				status = 429
				retry := q.Get("retry_after")
				if retry == "" {
					retry = "5"
				}
				w.Header().Set("Retry-After", retry)
			}
		default:
			status = 404
		}
		w.WriteHeader(status)
		if err := json.NewEncoder(w).Encode(map[string]any{"code": code, "attempt": count, "idempotency_key": key}); err != nil {
			slog.Debug("mock_response_failed")
		}
	})
	s := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 65 * time.Second, MaxHeaderBytes: 16384}
	if err := s.ListenAndServe(); err != nil {
		slog.Error("mock_stopped", "error", err)
	}
}
