// mock-target serves deterministic HTTP responses and request observations.
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
	records := map[string][]map[string]any{}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("GET /_admin/requests/{key}", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(records[r.PathValue("key")])
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Idempotency-Key")
		if key == "" {
			key = r.Header.Get("X-Notification-ID")
		}
		var body map[string]any
		_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20)).Decode(&body)
		mu.Lock()
		if len(counts) > 100000 {
			counts = map[string]int{}
			records = map[string][]map[string]any{}
		}
		counts[key]++
		count := counts[key]
		records[key] = append(records[key], map[string]any{"received_at": time.Now().UTC(), "notification_id": r.Header.Get("X-Notification-ID"), "idempotency_key": key, "body_request_id": body["request_id"], "path": r.URL.Path, "attempt": count})
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
