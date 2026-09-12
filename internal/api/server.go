package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"notifier/internal/application"
	"notifier/internal/auth"
	"notifier/internal/config"
	"notifier/internal/domain"
	"notifier/internal/repository/business"
)

type Server struct{ Service *application.Service }
type identity struct {
	client    string
	snapshot  *config.Snapshot
	requestID string
}
type identityKey struct{}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/notifications", s.submit)
	mux.HandleFunc("POST /api/v1/notifications:batch", s.batch)
	mux.HandleFunc("GET /api/v1/notifications", s.list)
	mux.HandleFunc("GET /api/v1/notifications/{id}", s.get)
	mux.HandleFunc("POST /api/v1/notifications/{action}", s.retry)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := domain.NewID().String()
		w.Header().Set("X-Request-ID", id)
		w.Header().Set("Cache-Control", "no-store")
		snap := s.Service.Config.Current()
		client, err := auth.Authenticate(r.Header.Get("Authorization"), snap)
		if err != nil {
			writeError(w, id, application.Fail("UNAUTHORIZED", 401, "valid bearer JWT required"))
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 40*time.Second)
		defer cancel()
		ctx = context.WithValue(ctx, identityKey{}, identity{client, snap, id})
		defer func() {
			if p := recover(); p != nil {
				slog.Error("api_panic", "request_id", id)
				writeError(w, id, application.Fail("INTERNAL_ERROR", 500, "internal error"))
			}
		}()
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}
func caller(r *http.Request) identity { return r.Context().Value(identityKey{}).(identity) }
func decode(w http.ResponseWriter, r *http.Request, v any) error {
	if ct := strings.Split(r.Header.Get("Content-Type"), ";")[0]; ct != "application/json" {
		return application.Fail("INVALID_REQUEST", 415, "Content-Type must be application/json")
	}
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return application.Fail("INVALID_REQUEST", 413, "request exceeds 2 MiB")
		}
		return application.Fail("INVALID_REQUEST", 400, "invalid JSON request")
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return application.Fail("INVALID_REQUEST", 400, "expected one JSON object")
	}
	return nil
}
func respond(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Debug("response_write_failed")
	}
}
func writeError(w http.ResponseWriter, id string, err error) {
	e := &application.Error{Code: "DEPENDENCY_UNAVAILABLE", Status: 503, Message: "dependency unavailable"}
	var app *application.Error
	switch {
	case errors.As(err, &app):
		e = app
	case errors.Is(err, domain.ErrNotFound):
		e = &application.Error{Code: "NOTIFICATION_NOT_FOUND", Status: 404, Message: "notification not found"}
	case errors.Is(err, domain.ErrConflict):
		e = &application.Error{Code: "IDEMPOTENCY_CONFLICT", Status: 409, Message: "key already used for a different request"}
	case errors.Is(err, domain.ErrState):
		e = &application.Error{Code: "INVALID_STATE", Status: 409, Message: "notification cannot be retried"}
	}
	if e.Status == 429 {
		w.Header().Set("Retry-After", "1")
	}
	if e.Status == 401 {
		w.Header().Set("WWW-Authenticate", "Bearer")
	}
	respond(w, e.Status, map[string]any{"error": map[string]string{"code": e.Code, "message": e.Message, "request_id": id}})
}
func (s *Server) submit(w http.ResponseWriter, r *http.Request) {
	a := caller(r)
	var in domain.Submission
	if err := decode(w, r, &in); err != nil {
		writeError(w, a.requestID, err)
		return
	}
	if in.Key != "" {
		writeError(w, a.requestID, application.Fail("INVALID_REQUEST", 400, "use Idempotency-Key header for single submissions"))
		return
	}
	in.Key = r.Header.Get("Idempotency-Key")
	n, created, err := s.Service.Accept(r.Context(), a.client, in, nil, a.snapshot)
	if err != nil {
		writeError(w, a.requestID, err)
		return
	}
	if created && n.Mode == "proxy" && s.Service.Proxy != nil {
		if err = s.Service.Proxy(r.Context(), n.ID, n.Generation); err != nil {
			slog.Warn("proxy_attempt_deferred", "notification_id", n.ID)
		}
		if fresh, e := s.Service.Store.Get(r.Context(), n.ID, a.client); e == nil {
			n = fresh
		}
	}
	status := 202
	if n.Status == "delivered" || n.Status == "failed" {
		status = 200
	}
	w.Header().Set("Location", "/api/v1/notifications/"+n.ID.String())
	if n.Mode == "proxy" && status == 200 {
		respond(w, status, map[string]any{"notification_id": n.ID, "status": n.Status, "target": n.Target, "accepted_at": n.Created, "proxy_result": map[string]any{"status": n.Status, "http_status": n.LastStatus, "error_code": n.LastError}})
		return
	}
	respond(w, status, n)
}
func (s *Server) batch(w http.ResponseWriter, r *http.Request) {
	a := caller(r)
	var body struct {
		Items []json.RawMessage `json:"items"`
	}
	if err := decode(w, r, &body); err != nil {
		writeError(w, a.requestID, err)
		return
	}
	if len(body.Items) == 0 || len(body.Items) > 100 {
		writeError(w, a.requestID, application.Fail("INVALID_REQUEST", 400, "batch must contain 1..100 items"))
		return
	}
	id := domain.NewID()
	if err := s.Service.Store.CreateBatch(r.Context(), id, a.client, len(body.Items)); err != nil {
		writeError(w, a.requestID, err)
		return
	}
	results := make([]map[string]any, 0, len(body.Items))
	for _, raw := range body.Items {
		var in domain.Submission
		err := config.Decode(raw, &in)
		var n *domain.Notification
		if err != nil {
			err = application.Fail("INVALID_REQUEST", 400, "invalid batch item")
		} else {
			n, _, err = s.Service.Accept(r.Context(), a.client, in, &id, a.snapshot)
		}
		out := map[string]any{"idempotency_key": in.Key, "accepted": err == nil}
		if err == nil {
			out["notification_id"] = n.ID
		} else {
			code := "DEPENDENCY_UNAVAILABLE"
			var e *application.Error
			if errors.As(err, &e) {
				code = e.Code
			} else if errors.Is(err, domain.ErrConflict) {
				code = "IDEMPOTENCY_CONFLICT"
			}
			out["error"] = map[string]string{"code": code}
		}
		results = append(results, out)
	}
	respond(w, 202, map[string]any{"batch_id": id, "results": results})
}
func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	a := caller(r)
	id, err := domain.ParseID(r.PathValue("id"))
	if err != nil {
		writeError(w, a.requestID, application.Fail("INVALID_REQUEST", 400, "invalid notification ID"))
		return
	}
	n, err := s.Service.Store.Get(r.Context(), id, a.client)
	if err != nil {
		writeError(w, a.requestID, err)
		return
	}
	attempts, err := s.Service.Store.Attempts(r.Context(), id)
	if err != nil {
		writeError(w, a.requestID, err)
		return
	}
	respond(w, 200, struct {
		*domain.Notification
		Attempts []business.Attempt `json:"attempts"`
	}{n, attempts})
}
func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	a := caller(r)
	q := r.URL.Query()
	f := business.Filter{Status: q.Get("status"), Target: q.Get("target"), Limit: 50}
	bad := func() {
		writeError(w, a.requestID, application.Fail("INVALID_REQUEST", 400, "invalid query filter or cursor"))
	}
	if f.Status != "" && !slices.Contains([]string{"pending", "in_flight", "delivered", "failed"}, f.Status) {
		bad()
		return
	}
	if v := q.Get("limit"); v != "" {
		i, e := strconv.Atoi(v)
		if e != nil || i < 1 || i > 100 {
			bad()
			return
		}
		f.Limit = i
	}
	if v := q.Get("batch_id"); v != "" {
		id, e := domain.ParseID(v)
		if e != nil {
			bad()
			return
		}
		f.Batch = &id
	}
	if v := q.Get("cursor"); v != "" {
		b, e := base64.RawURLEncoding.DecodeString(v)
		if e != nil {
			bad()
			return
		}
		id, e := domain.ParseID(string(b))
		if e != nil {
			bad()
			return
		}
		f.Cursor = &id
	}
	limit := f.Limit
	f.Limit++
	items, err := s.Service.Store.List(r.Context(), a.client, f)
	if err != nil {
		writeError(w, a.requestID, err)
		return
	}
	cursor := ""
	if len(items) > limit {
		items = items[:limit]
		cursor = base64.RawURLEncoding.EncodeToString([]byte(items[len(items)-1].ID.String()))
	}
	respond(w, 200, map[string]any{"items": items, "next_cursor": cursor})
}
func (s *Server) retry(w http.ResponseWriter, r *http.Request) {
	a := caller(r)
	action := r.PathValue("action")
	if !strings.HasSuffix(action, ":retry") {
		writeError(w, a.requestID, domain.ErrNotFound)
		return
	}
	id, err := domain.ParseID(strings.TrimSuffix(action, ":retry"))
	if err != nil {
		writeError(w, a.requestID, application.Fail("INVALID_REQUEST", 400, "invalid notification ID"))
		return
	}
	if s.Service.ManualRetry == nil {
		writeError(w, a.requestID, application.Fail("DEPENDENCY_UNAVAILABLE", 503, "retry unavailable"))
		return
	}
	if err = s.Service.ManualRetry(r.Context(), a.client, id, a.snapshot); err != nil {
		writeError(w, a.requestID, err)
		return
	}
	n, err := s.Service.Store.Get(r.Context(), id, a.client)
	if err != nil {
		writeError(w, a.requestID, err)
		return
	}
	slog.Info("manual_retry_audit", "client_id", a.client, "notification_id", id, "request_id", a.requestID, "generation", n.Generation)
	respond(w, 202, n)
}
