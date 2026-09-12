package application

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"notifier/internal/config"
	"notifier/internal/domain"
	"notifier/internal/repository/business"
)

type Error struct {
	Code    string
	Status  int
	Message string
}

func (e *Error) Error() string                           { return e.Code }
func Fail(code string, status int, message string) error { return &Error{code, status, message} }

type Ingress interface {
	Ingress(context.Context, string, config.Quota) (bool, error)
}
type Service struct {
	Store       *business.Store
	Config      *config.Manager
	Quota       Ingress
	Proxy       func(context.Context, domain.ID, uint64) error
	ManualRetry func(context.Context, string, domain.ID, *config.Snapshot) error
	Accepted    atomic.Uint64
}

func (s *Service) Accept(ctx context.Context, client string, in domain.Submission, batch *domain.ID, snap *config.Snapshot) (*domain.Notification, bool, error) {
	if len(in.Key) == 0 || len(in.Key) > 128 || strings.TrimSpace(in.Key) != in.Key || strings.ContainsAny(in.Key, "\r\n\x00") {
		return nil, false, Fail("INVALID_REQUEST", 400, "invalid idempotency key")
	}
	in.Request.Method = strings.ToUpper(in.Request.Method)
	if err := normalize(&in.Request); err != nil {
		return nil, false, Fail("INVALID_REQUEST", 400, err.Error())
	}
	if in.Mode != "" && in.Mode != "async" && in.Mode != "proxy" {
		return nil, false, Fail("INVALID_REQUEST", 400, "invalid delivery mode")
	}
	// Keep omitted mode stable across configuration reloads when hashing.
	key := in.Key
	in.Key = ""
	encoded, err := json.Marshal(in)
	if err != nil {
		return nil, false, err
	}
	hash := sha256.Sum256(encoded)
	old, err := s.Store.ByKey(ctx, client, key)
	if err == nil {
		if old.Hash != hash {
			return nil, false, domain.ErrConflict
		}
		return old, false, nil
	}
	if !errors.Is(err, domain.ErrNotFound) {
		return nil, false, err
	}
	t, err := Allowed(snap, client, in.Target)
	if err != nil {
		return nil, false, err
	}
	if !slices.Contains(t.Endpoint.Methods, in.Request.Method) || !config.PathAllowed(t.Endpoint.Prefixes, in.Request.Path) {
		return nil, false, Fail("TARGET_NOT_ALLOWED", 403, "request violates target allowlist")
	}
	for k := range in.Request.Headers {
		if strings.EqualFold(k, t.Auth.Header) {
			return nil, false, Fail("INVALID_REQUEST", 400, "authentication headers must use secret references")
		}
	}
	if in.Mode == "" {
		in.Mode = t.DefaultMode
	}
	if in.Mode == "proxy" && !t.AllowProxy {
		return nil, false, Fail("TARGET_NOT_ALLOWED", 403, "proxy is disabled")
	}
	if s.Quota != nil {
		ok, err := s.Quota.Ingress(ctx, client, snap.Quotas[snap.Clients[client].Quota])
		if err != nil {
			return nil, false, Fail("DEPENDENCY_UNAVAILABLE", 503, "quota unavailable")
		}
		if !ok {
			return nil, false, Fail("QUOTA_EXCEEDED", 429, "ingress quota exceeded")
		}
	}
	now := time.Now().UTC()
	r := snap.Retries[t.RetryPolicy]
	n := &domain.Notification{ID: domain.NewID(), Client: client, Target: in.Target, Batch: batch, Key: key, Hash: hash, Request: in.Request, Mode: in.Mode, Status: "pending", Generation: 1, Next: now, Deadline: now.Add(r.MaxDuration.Duration()), Created: now, Updated: now, TargetRevision: t.Revision, RetryRevision: r.Revision, ConfigRevision: snap.Revision}
	if t.Hook != "" {
		n.HookRevision = snap.Hooks[t.Hook].Revision
	}
	n, created, err := s.Store.Accept(ctx, n)
	if err != nil {
		return nil, false, err
	}
	if created {
		s.Accepted.Add(1)
		slog.Info("notification_accepted", "request_id", domain.RequestID(ctx), "notification_id", n.ID, "batch_id", n.Batch, "client_id", client, "target_id", n.Target, "config_revision", snap.Revision)
	}
	return n, created, nil
}
func Allowed(s *config.Snapshot, client, target string) (config.Target, error) {
	t, ok := s.Targets[target]
	if !ok {
		return t, Fail("TARGET_NOT_FOUND", 404, "target not found")
	}
	c, ok := s.Clients[client]
	if !ok || !c.Enabled || !slices.Contains(c.Targets, target) {
		return t, Fail("TARGET_NOT_ALLOWED", 403, "target is not enabled for client")
	}
	if !t.Enabled {
		return t, Fail("TARGET_DISABLED", 403, "target disabled")
	}
	return t, nil
}
func normalize(r *domain.Request) error {
	if err := config.ValidatePath(r.Path); err != nil {
		return err
	}
	if r.BodyEncoding == "" {
		r.BodyEncoding = "utf8"
	}
	var body []byte
	switch r.BodyEncoding {
	case "utf8":
		if !utf8.ValidString(r.Body) {
			return errors.New("body must be UTF-8")
		}
		body = []byte(r.Body)
	case "base64":
		var err error
		body, err = base64.StdEncoding.DecodeString(r.Body)
		if err != nil {
			return errors.New("invalid base64 body")
		}
		r.Body = base64.StdEncoding.EncodeToString(body)
	default:
		return errors.New("invalid body encoding")
	}
	if len(body) > 1<<20 {
		return errors.New("notification body exceeds 1 MiB")
	}
	if utf8.Valid(body) {
		r.BodyEncoding = "utf8"
		r.Body = string(body)
	}
	if len(r.Headers) > 64 || len(r.Query) > 128 {
		return errors.New("too many headers or query parameters")
	}
	headers := map[string]string{}
	total := 0
	for k, v := range r.Headers {
		if !config.ValidHeader(k) || config.ForbiddenTransportHeader(k) || config.SensitiveHeader(k) || strings.ContainsAny(v, "\r\n\x00") {
			return errors.New("invalid or sensitive request header")
		}
		k = http.CanonicalHeaderKey(k)
		if _, ok := headers[k]; ok {
			return errors.New("duplicate normalized header")
		}
		headers[k] = v
		total += len(k) + len(v)
	}
	for k, v := range r.Query {
		if k == "" || strings.ContainsAny(k+v, "\r\n\x00") || config.SensitiveHeader(k) {
			return errors.New("invalid or sensitive query parameter")
		}
		total += len(k) + len(v)
	}
	if total > 16384 {
		return errors.New("headers and query too large")
	}
	r.Headers = headers
	return nil
}
