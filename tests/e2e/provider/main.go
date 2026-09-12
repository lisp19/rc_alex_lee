// This is a real HTTP supplier simulator for end-to-end cases, not a unit-test
// mock. Each case controls its own response sequence through query parameters.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type observation struct {
	At           time.Time `json:"at"`
	Notification string    `json:"notification"`
	HeaderKey    string    `json:"header_key"`
	QueryKey     string    `json:"query_key"`
	Body         any       `json:"body"`
	BodyHash     string    `json:"body_hash"`
	AuthHash     string    `json:"auth_hash"`
	Number       int       `json:"number"`
	Active       int       `json:"active"`
	Status       int       `json:"status"`
}

func digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func level(s string, n, defaultValue int) int {
	if s == "" {
		return defaultValue
	}
	parts := strings.Split(s, ",")
	value, err := strconv.Atoi(parts[min(n-1, len(parts)-1)])
	if err != nil {
		return defaultValue
	}
	return value
}
func main() {
	var mu sync.Mutex
	observations := map[string][]observation{}
	active, peak, redirects := 0, 0, 0
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("GET /observations", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		rows := append([]observation{}, observations[r.URL.Query().Get("case")]...)
		p, rd := peak, redirects
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"requests": rows, "peak": p, "redirects": rd})
	})
	mux.HandleFunc("/redirect-hit", func(w http.ResponseWriter, r *http.Request) { mu.Lock(); redirects++; mu.Unlock(); w.WriteHeader(200) })
	mux.HandleFunc("/deliver", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 2<<20))
		if err != nil {
			http.Error(w, "body limit", 413)
			return
		}
		q := r.URL.Query()
		key := q.Get("case")
		var parsed any
		if len(body) <= 8192 {
			_ = json.Unmarshal(body, &parsed)
		}
		mu.Lock()
		n := len(observations[key]) + 1
		active++
		peak = max(peak, active)
		status := level(q.Get("statuses"), n, 200)
		observations[key] = append(observations[key], observation{At: time.Now().UTC(), Notification: r.Header.Get("X-Notification-ID"), HeaderKey: r.Header.Get("Idempotency-Key"), QueryKey: q.Get("partner_id"), Body: parsed, BodyHash: digest(body), AuthHash: digest([]byte(r.Header.Get("Authorization"))), Number: n, Active: active, Status: status})
		mu.Unlock()
		defer func() { mu.Lock(); active--; mu.Unlock() }()
		if delay := level(q.Get("delays_ms"), n, 0); delay > 0 {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(time.Duration(min(delay, 30000)) * time.Millisecond):
			}
		}
		if n <= level(q.Get("resets"), 1, 0) {
			if h, ok := w.(http.Hijacker); ok {
				conn, _, err := h.Hijack()
				if err == nil {
					_ = conn.Close()
				}
			}
			return
		}
		if retry := q.Get("retry_after"); retry != "" {
			w.Header().Set("Retry-After", retry)
		}
		if status >= 300 && status < 400 {
			w.Header().Set("Location", "http://e2e-provider:8080/redirect-hit")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if size := level(q.Get("response_size"), 1, 0); size > 0 {
			_, _ = io.WriteString(w, strings.Repeat("x", min(size, 2<<20)))
			return
		}
		if q.Get("malformed") == "true" {
			_, _ = io.WriteString(w, "{bad-json")
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"code": level(q.Get("codes"), n, 0), "password": "fixture-password", "email": "person@example.test", "authorization": r.Header.Get("Authorization"), "reflected": r.Header.Get("Authorization")})
	})
	s := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 35 * time.Second}
	log.Fatal(s.ListenAndServe())
}
