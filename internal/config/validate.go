package config

import (
	"bytes"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/netip"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/cel-go/cel"
)

var namePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`)

func Decode(b []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return errors.New("expected one JSON document")
	}
	return nil
}
func Build(revision uint64, b []byte) (*Snapshot, error) {
	s := &Snapshot{Revision: revision, LoadedAt: time.Now().UTC(), Programs: map[string]cel.Program{}, Keys: map[string]map[string]any{}}
	if err := Decode(b, &s.Document); err != nil {
		return nil, err
	}
	if s.Targets == nil || s.Clients == nil || s.Retries == nil || s.Quotas == nil || s.Hooks == nil || s.Issuers == nil {
		return nil, errors.New("all management entity maps are required")
	}
	if len(s.Targets) > 10000 || len(s.Clients) > 10000 || len(s.Hooks) > 1000 || len(b) > 16<<20 {
		return nil, errors.New("configuration size limit exceeded")
	}
	env, err := cel.NewEnv(cel.Variable("status", cel.IntType), cel.Variable("headers", cel.MapType(cel.StringType, cel.StringType)), cel.Variable("body", cel.StringType), cel.Variable("json", cel.DynType), cel.Variable("notification", cel.MapType(cel.StringType, cel.StringType)), cel.Variable("attempt", cel.IntType), cel.ParserRecursionLimit(32), cel.ParserExpressionSizeLimit(8192))
	if err != nil {
		return nil, err
	}
	for id, h := range s.Hooks {
		if !namePattern.MatchString(id) || h.Revision == 0 {
			return nil, fmt.Errorf("invalid hook %s", id)
		}
		ast, issues := env.Compile(h.Expression)
		if issues.Err() != nil {
			return nil, fmt.Errorf("hook %s: %w", id, issues.Err())
		}
		if len(ast.SourceInfo().GetPositions()) > 512 {
			return nil, fmt.Errorf("hook %s AST too large", id)
		}
		if typ := ast.OutputType().TypeName(); typ != "map" && typ != "dyn" {
			return nil, fmt.Errorf("hook %s must return a map", id)
		}
		p, err := env.Program(ast, cel.CostLimit(10000), cel.InterruptCheckFrequency(32))
		if err != nil {
			return nil, err
		}
		s.Programs[id] = p
	}
	for id, r := range s.Retries {
		if !namePattern.MatchString(id) || r.Revision == 0 || r.MaxDuration.Duration() <= 0 || r.MaxDuration.Duration() > 30*24*time.Hour || len(r.Delays) > 100 {
			return nil, fmt.Errorf("invalid retry policy %s", id)
		}
		for _, d := range r.Delays {
			if !slices.Contains(Buckets, d.Duration()) {
				return nil, fmt.Errorf("retry %s: unsupported delay", id)
			}
		}
	}
	var global *Quota
	for id, q := range s.Quotas {
		if !namePattern.MatchString(id) || (q.FailMode != "open" && q.FailMode != "closed") || q.IngressGlobal < 0 || q.IngressClient < 0 || q.EgressTarget < 0 || q.EgressClientTarget < 0 || q.GlobalConcurrency < 0 || q.TargetConcurrency < 0 {
			return nil, fmt.Errorf("invalid quota %s", id)
		}
		if global == nil {
			copy := q
			global = &copy
		} else if global.IngressGlobal != q.IngressGlobal || global.GlobalConcurrency != q.GlobalConcurrency {
			return nil, errors.New("global quota limits must agree across policies")
		}
	}
	for id, t := range s.Targets {
		if !namePattern.MatchString(id) || t.Revision == 0 || t.Adapter != "generic_http" {
			return nil, fmt.Errorf("invalid target or unregistered adapter %s", id)
		}
		if err := ValidateEndpoint(t.Endpoint); err != nil {
			return nil, fmt.Errorf("target %s: %w", id, err)
		}
		if _, ok := s.Retries[t.RetryPolicy]; !ok {
			return nil, fmt.Errorf("target %s retry missing", id)
		}
		if _, ok := s.Quotas[t.Quota]; !ok {
			return nil, fmt.Errorf("target %s quota missing", id)
		}
		if t.Hook != "" {
			if _, ok := s.Hooks[t.Hook]; !ok {
				return nil, fmt.Errorf("target %s hook missing", id)
			}
		}
		if (t.DefaultMode != "async" && t.DefaultMode != "proxy") || (t.DefaultMode == "proxy" && !t.AllowProxy) || t.PreviewBytes < 0 || t.PreviewBytes > 8192 {
			return nil, fmt.Errorf("target %s invalid delivery/preview", id)
		}
		if t.Auth.Type != "none" && t.Auth.Type != "static_header" {
			return nil, errors.New("unsupported auth type")
		}
		if t.Auth.Type == "static_header" && (!ValidHeader(t.Auth.Header) || !namePattern.MatchString(t.Auth.SecretRef) || ForbiddenTransportHeader(t.Auth.Header)) {
			return nil, errors.New("invalid secret header")
		}
		if !slices.Contains([]string{"supported", "unsupported", "unknown"}, t.Idempotency.Mode) {
			return nil, errors.New("invalid idempotency mode")
		}
		if t.Idempotency.Mode == "supported" && len(t.Idempotency.Inject) == 0 {
			return nil, errors.New("supported idempotency requires injection")
		}
		if t.Idempotency.Mode != "supported" && len(t.Idempotency.Inject) > 0 {
			return nil, errors.New("idempotency injection requires supported mode")
		}
		if t.RetryEnabled && t.Idempotency.Mode != "supported" {
			if t.Idempotency.UncertainRetry == nil {
				return nil, errors.New("non-idempotent retry requires explicit allow_uncertain_retry")
			}
			slog.Warn("non_idempotent_retry", "target_id", id)
		}
		for _, in := range t.Idempotency.Inject {
			switch in.Location {
			case "header":
				if !ValidHeader(in.Key) || ForbiddenTransportHeader(in.Key) || strings.EqualFold(in.Key, t.Auth.Header) || SensitiveHeader(in.Key) {
					return nil, errors.New("invalid injection header")
				}
			case "query":
				if in.Key == "" || len(in.Key) > 128 {
					return nil, errors.New("invalid injection query")
				}
			case "body_json":
				if !strings.HasPrefix(in.Pointer, "/") || len(in.Pointer) > 512 {
					return nil, errors.New("invalid JSON pointer")
				}
				for i := 0; i < len(in.Pointer); i++ {
					if in.Pointer[i] == '~' {
						if i+1 >= len(in.Pointer) || (in.Pointer[i+1] != '0' && in.Pointer[i+1] != '1') {
							return nil, errors.New("invalid JSON pointer escape")
						}
						i++
					}
				}
			default:
				return nil, errors.New("unsupported idempotency injection")
			}
		}
	}
	for id, c := range s.Clients {
		if !namePattern.MatchString(id) {
			return nil, errors.New("invalid client id")
		}
		if _, ok := s.Quotas[c.Quota]; !ok {
			return nil, errors.New("client quota missing")
		}
		for _, t := range c.Targets {
			if _, ok := s.Targets[t]; !ok {
				return nil, errors.New("client target missing")
			}
		}
	}
	for iss, c := range s.Issuers {
		if iss == "" || c.Audience == "" || len(c.Keys) == 0 {
			return nil, errors.New("invalid issuer")
		}
		s.Keys[iss] = map[string]any{}
		for kid, pem := range c.Keys {
			var key any
			var err error
			switch c.Algorithm {
			case "RS256":
				key, err = jwt.ParseRSAPublicKeyFromPEM([]byte(pem))
			case "ES256":
				key, err = jwt.ParseECPublicKeyFromPEM([]byte(pem))
			default:
				return nil, errors.New("only RS256/ES256 accepted")
			}
			if err != nil {
				return nil, fmt.Errorf("issuer %s key %s: %w", iss, kid, err)
			}
			if k, ok := key.(*rsa.PublicKey); ok && k.N.BitLen() < 2048 {
				return nil, errors.New("RSA key must be at least 2048 bits")
			}
			s.Keys[iss][kid] = key
		}
	}
	return s, nil
}
func ValidateEndpoint(e Endpoint) error {
	u, err := url.Parse(e.BaseURL)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return errors.New("base_url must be an origin")
	}
	if u.Scheme != "https" && !(e.AllowHTTP && u.Scheme == "http") {
		return errors.New("HTTPS required")
	}
	if !slices.Contains(e.Hosts, strings.ToLower(u.Hostname())) {
		return errors.New("host not allowed")
	}
	if len(e.Methods) == 0 || len(e.Prefixes) == 0 {
		return errors.New("method and path allowlists required")
	}
	for _, m := range e.Methods {
		if !slices.Contains([]string{"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"}, m) {
			return errors.New("invalid method")
		}
	}
	for _, p := range e.Prefixes {
		if err := ValidatePath(p); err != nil {
			return err
		}
	}
	for _, c := range e.CIDRs {
		if _, err := netip.ParsePrefix(c); err != nil {
			return err
		}
	}
	if e.ConnectTimeout.Duration() <= 0 || e.ConnectTimeout.Duration() > 10*time.Second || e.RequestTimeout.Duration() <= 0 || e.RequestTimeout.Duration() > 30*time.Second || e.ConnectTimeout > e.RequestTimeout {
		return errors.New("timeouts must fit 60s lease")
	}
	if e.MaxResponse <= 0 || e.MaxResponse > 16<<20 {
		return errors.New("invalid response limit")
	}
	return nil
}
func ValidatePath(p string) error {
	if !strings.HasPrefix(p, "/") || strings.HasPrefix(p, "//") || len(p) > 2048 || strings.ContainsAny(p, "\\?#%\r\n\x00") {
		return errors.New("invalid relative path (use decoded path)")
	}
	for _, part := range strings.Split(p, "/") {
		if part == "." || part == ".." {
			return errors.New("path traversal forbidden")
		}
	}
	return nil
}

func PathAllowed(prefixes []string, p string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(p, prefix) {
			return true
		}
	}
	return false
}
func ValidHeader(s string) bool {
	if s == "" || len(s) > 128 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("!#$%&'*+-.^_`|~", c)) {
			return false
		}
	}
	return true
}
func ForbiddenTransportHeader(s string) bool {
	return slices.Contains([]string{"host", "connection", "content-length", "transfer-encoding", "trailer", "te", "upgrade", "proxy-authorization", "proxy-connection"}, strings.ToLower(s))
}
func SensitiveHeader(s string) bool {
	s = strings.ReplaceAll(strings.ToLower(s), "_", "-")
	return strings.Contains(s, "authorization") || strings.Contains(s, "cookie") || strings.Contains(s, "api-key") || strings.Contains(s, "apikey") || strings.Contains(s, "token") || strings.Contains(s, "secret") || strings.Contains(s, "password") || strings.Contains(s, "passwd")
}
