// Package target provides compiled adapters. All adapters are invoked by the
// delivery orchestrator; they never own retry loops or task state.
package target

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"notifier/internal/config"
	"notifier/internal/domain"
)

type Adapter interface {
	Deliver(context.Context, *domain.Notification, config.Target, config.Target, string) domain.Decision
}

var errPolicyBlocked = errors.New("destination blocked by policy")

type HTTP struct {
	mu         sync.Mutex
	transports map[string]*http.Transport
}

func NewHTTP() *HTTP { return &HTTP{transports: map[string]*http.Transport{}} }
func (h *HTTP) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, t := range h.transports {
		t.CloseIdleConnections()
	}
}

// IP policy is evaluated on every new connection and the validated literal IP
// is dialed directly. Environment proxies and redirects cannot bypass it.
func allowedIP(ip netip.Addr, cidrs []string) bool {
	ip = ip.Unmap()
	if !ip.IsValid() || ip.IsUnspecified() || ip.IsMulticast() {
		return false
	}
	for _, s := range cidrs {
		p, err := netip.ParsePrefix(s)
		if err == nil && p.Contains(ip) {
			return true
		}
	}
	if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return false
	}
	for _, s := range []string{"0.0.0.0/8", "100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "240.0.0.0/4", "2001:db8::/32", "64:ff9b::/96", "64:ff9b:1::/48", "2002::/16", "2001::/32"} {
		if netip.MustParsePrefix(s).Contains(ip) {
			return false
		}
	}
	return true
}
func (h *HTTP) transport(behavior, security config.Endpoint) *http.Transport {
	b, _ := json.Marshal(struct{ B, S config.Endpoint }{behavior, security})
	key := string(b)
	h.mu.Lock()
	defer h.mu.Unlock()
	if t := h.transports[key]; t != nil {
		return t
	}
	t := &http.Transport{Proxy: nil, MaxIdleConns: 128, MaxIdleConnsPerHost: 32, MaxConnsPerHost: 64, IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: behavior.ConnectTimeout.Duration(), ResponseHeaderTimeout: behavior.RequestTimeout.Duration(), MaxResponseHeaderBytes: 16384, DisableCompression: true, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
	t.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		if !slices.Contains(security.Hosts, strings.ToLower(host)) {
			return nil, errPolicyBlocked
		}
		c, cancel := context.WithTimeout(ctx, behavior.ConnectTimeout.Duration())
		defer cancel()
		ips, err := net.DefaultResolver.LookupNetIP(c, "ip", host)
		if err != nil {
			return nil, err
		}
		if len(ips) == 0 {
			return nil, errors.New("empty DNS answer")
		}
		for _, ip := range ips {
			if !allowedIP(ip, security.CIDRs) {
				return nil, errPolicyBlocked
			}
		}
		var last error
		for _, ip := range ips {
			conn, err := (&net.Dialer{}).DialContext(c, network, net.JoinHostPort(ip.Unmap().String(), port))
			if err == nil {
				return conn, nil
			}
			last = err
		}
		return nil, last
	}
	if len(h.transports) >= 128 {
		for k, v := range h.transports {
			v.CloseIdleConnections()
			delete(h.transports, k)
			break
		}
	}
	h.transports[key] = t
	return t
}
func (h *HTTP) Deliver(ctx context.Context, n *domain.Notification, t, current config.Target, secret string) domain.Decision {
	d := domain.Decision{Action: "fail", Started: time.Now().UTC()}
	finish := func(reason string) domain.Decision { d.Reason = reason; d.Latency = time.Since(d.Started); return d }
	if !current.Enabled || !slices.Contains(current.Endpoint.Methods, n.Request.Method) || !config.PathAllowed(current.Endpoint.Prefixes, n.Request.Path) || config.ValidatePath(n.Request.Path) != nil {
		return finish("target_policy_blocked")
	}
	u, err := url.Parse(t.Endpoint.BaseURL)
	if err != nil {
		return finish("invalid_target")
	}
	// Origin changes revoke old destinations immediately; behavioral revisions
	// still govern retry, hooks and idempotency.
	latest, err := url.Parse(current.Endpoint.BaseURL)
	if err != nil || u.Scheme != latest.Scheme || u.Host != latest.Host {
		return finish("target_origin_changed")
	}
	u.Path = n.Request.Path
	u.RawPath = ""
	query := url.Values{}
	for k, v := range n.Request.Query {
		query.Set(k, v)
	}
	body := []byte(n.Request.Body)
	if n.Request.BodyEncoding == "base64" {
		body, err = base64.StdEncoding.DecodeString(n.Request.Body)
		if err != nil {
			return finish("invalid_body")
		}
	}
	headers := http.Header{}
	for k, v := range n.Request.Headers {
		if config.ForbiddenTransportHeader(k) || config.SensitiveHeader(k) || strings.EqualFold(k, current.Auth.Header) {
			return finish("invalid_header")
		}
		headers.Set(k, v)
	}
	if t.Idempotency.Mode == "supported" {
		for _, in := range t.Idempotency.Inject {
			switch in.Location {
			case "header":
				headers.Set(in.Key, n.Key)
			case "query":
				query.Set(in.Key, n.Key)
			case "body_json":
				body, err = injectJSON(body, in.Pointer, n.Key)
				if err != nil {
					return finish("idempotency_injection_failed")
				}
				headers.Set("Content-Type", "application/json")
			}
		}
	}
	u.RawQuery = query.Encode()
	ctx, cancel := context.WithTimeout(ctx, t.Endpoint.RequestTimeout.Duration())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, n.Request.Method, u.String(), bytes.NewReader(body))
	if err != nil {
		return finish("request_build_failed")
	}
	req.Header = headers
	req.Header.Set("X-Notification-ID", n.ID.String())
	req.Header.Set("X-Request-ID", domain.RequestID(ctx))
	req.Header.Set("User-Agent", "notifier/1")
	if current.Auth.Type == "static_header" {
		if secret == "" {
			return finish("secret_unavailable")
		}
		req.Header.Set(current.Auth.Header, secret)
	}
	client := http.Client{Transport: h.transport(t.Endpoint, current.Endpoint), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		if errors.Is(err, errPolicyBlocked) {
			return finish("destination_blocked")
		}
		var ne net.Error
		if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EPIPE) || (errors.As(err, &ne) && (ne.Timeout() || ne.Temporary())) {
			d.Action = "retry"
			d.Uncertain = true
			return finish("network_error")
		}
		return finish("permanent_transport_error")
	}
	defer resp.Body.Close()
	d.HTTPStatus = resp.StatusCode
	d.RetryAfter = resp.Header.Get("Retry-After")
	d.Headers = map[string]string{}
	for _, key := range []string{"Content-Type", "Retry-After"} {
		if v := resp.Header.Get(key); v != "" {
			d.Headers[key] = v
		}
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, t.Endpoint.MaxResponse+1))
	if err != nil {
		d.Action = "retry"
		d.Uncertain = true
		return finish("response_read_error")
	}
	if int64(len(raw)) > t.Endpoint.MaxResponse {
		return finish("response_too_large")
	}
	hash := sha256.Sum256(raw)
	d.BodyHash = hash[:]
	// Raw bounded preview is supplied to the hook, then redacted by delivery.
	d.Preview = raw[:min(len(raw), t.PreviewBytes)]
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		d.Action = "success"
	case resp.StatusCode == 408 || resp.StatusCode == 429 || resp.StatusCode >= 500 && resp.StatusCode < 600:
		d.Action = "retry"
	default:
		d.Action = "fail"
	}
	return finish("http_" + strconv.Itoa(resp.StatusCode))
}
func injectJSON(body []byte, pointer, key string) ([]byte, error) {
	var root any
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&root); err != nil {
		return nil, err
	}
	if dec.Decode(new(any)) != io.EOF {
		return nil, errors.New("invalid JSON")
	}
	parts := strings.Split(strings.TrimPrefix(pointer, "/"), "/")
	for i, p := range parts {
		parts[i] = strings.ReplaceAll(strings.ReplaceAll(p, "~1", "/"), "~0", "~")
	}
	root, err := setPointer(root, parts, key)
	if err != nil {
		return nil, err
	}
	return json.Marshal(root)
}

func setPointer(node any, parts []string, value string) (any, error) {
	if len(parts) == 0 {
		return value, nil
	}
	switch v := node.(type) {
	case map[string]any:
		child, exists := v[parts[0]]
		if !exists && len(parts) > 1 {
			child = map[string]any{}
		}
		next, err := setPointer(child, parts[1:], value)
		if err != nil {
			return nil, err
		}
		v[parts[0]] = next
		return v, nil
	case []any:
		if parts[0] == "-" && len(parts) == 1 {
			return append(v, value), nil
		}
		i, err := strconv.Atoi(parts[0])
		if err != nil || i < 0 || i >= len(v) || strconv.Itoa(i) != parts[0] {
			return nil, errors.New("invalid JSON array pointer")
		}
		next, err := setPointer(v[i], parts[1:], value)
		if err != nil {
			return nil, err
		}
		v[i] = next
		return v, nil
	default:
		return nil, errors.New("JSON pointer traverses scalar")
	}
}

// Redact never retains an unstructured body whose sensitive fields cannot be
// identified. Hash is of the complete original body, not the redacted preview.
func Redact(d *domain.Decision, t config.Target, secret string) {
	var body any
	if json.Unmarshal(d.Preview, &body) == nil {
		body = redactValue(body, t.SensitiveJSON, secret)
		b, err := json.Marshal(body)
		if err == nil {
			d.Preview = b[:min(len(b), t.PreviewBytes)]
		}
	} else {
		d.Preview = nil
	}
	if len(d.Report) > 0 {
		var report any
		if json.Unmarshal(d.Report, &report) == nil {
			report = redactValue(report, t.SensitiveJSON, secret)
			b, err := json.Marshal(report)
			if err == nil && len(b) <= 8192 {
				d.Report = b
			} else {
				d.Report = nil
			}
		} else {
			d.Report = nil
		}
	}
	for k, v := range d.Headers {
		if secret != "" {
			d.Headers[k] = strings.ReplaceAll(v, secret, "[REDACTED]")
		}
	}
	if secret != "" && strings.Contains(d.Reason, secret) {
		d.Reason = "redacted_reason"
	}
}
func redactValue(v any, fields []string, secret string) any {
	switch x := v.(type) {
	case map[string]any:
		for k, val := range x {
			if config.SensitiveHeader(k) || slices.Contains(fields, k) {
				x[k] = "[REDACTED]"
			} else if str, ok := val.(string); ok && secret != "" {
				x[k] = strings.ReplaceAll(str, secret, "[REDACTED]")
			} else {
				x[k] = redactValue(val, fields, secret)
			}
		}
	case []any:
		for i, val := range x {
			if str, ok := val.(string); ok && secret != "" {
				x[i] = strings.ReplaceAll(str, secret, "[REDACTED]")
			} else {
				x[i] = redactValue(val, fields, secret)
			}
		}
	case string:
		if secret != "" {
			return strings.ReplaceAll(x, secret, "[REDACTED]")
		}
	}
	return v
}
func BodyHashString(d domain.Decision) string { return hex.EncodeToString(d.BodyHash) }
