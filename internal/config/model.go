// Package config owns immutable, validated management snapshots. Callers must
// treat values returned by Manager.Current and History as read-only.
package config

import (
	"encoding/json"
	"fmt"
	"github.com/google/cel-go/cel"
	"time"
)

type Duration time.Duration

func (d Duration) Duration() time.Duration      { return time.Duration(d) }
func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	t, err := time.ParseDuration(s)
	*d = Duration(t)
	return err
}

type Document struct {
	Targets map[string]Target `json:"targets"`
	Clients map[string]Client `json:"clients"`
	Retries map[string]Retry  `json:"retry_policies"`
	Quotas  map[string]Quota  `json:"quota_policies"`
	Hooks   map[string]Hook   `json:"hooks"`
	Issuers map[string]Issuer `json:"issuers"`
}
type Target struct {
	Revision      uint64      `json:"revision"`
	Enabled       bool        `json:"enabled"`
	Adapter       string      `json:"adapter"`
	Endpoint      Endpoint    `json:"endpoint"`
	Auth          Auth        `json:"auth"`
	Idempotency   Idempotency `json:"idempotency"`
	RetryEnabled  bool        `json:"retry_enabled"`
	RetryPolicy   string      `json:"retry_policy"`
	Hook          string      `json:"hook,omitempty"`
	Quota         string      `json:"quota_policy"`
	DefaultMode   string      `json:"default_mode"`
	AllowProxy    bool        `json:"allow_proxy"`
	PreviewBytes  int         `json:"preview_bytes"`
	SensitiveJSON []string    `json:"sensitive_json_fields,omitempty"`
}
type Endpoint struct {
	BaseURL        string   `json:"base_url"`
	Hosts          []string `json:"allowed_hosts"`
	Methods        []string `json:"allowed_methods"`
	Prefixes       []string `json:"allowed_path_prefixes"`
	CIDRs          []string `json:"allowed_cidrs,omitempty"`
	AllowHTTP      bool     `json:"allow_http"`
	ConnectTimeout Duration `json:"connect_timeout"`
	RequestTimeout Duration `json:"request_timeout"`
	MaxResponse    int64    `json:"max_response_bytes"`
}
type Auth struct {
	Type      string `json:"type"`
	Header    string `json:"header,omitempty"`
	SecretRef string `json:"secret_ref,omitempty"`
}
type Idempotency struct {
	Mode           string      `json:"mode"`
	UncertainRetry *bool       `json:"allow_uncertain_retry,omitempty"`
	Inject         []Injection `json:"inject,omitempty"`
}
type Injection struct {
	Location string `json:"location"`
	Key      string `json:"key,omitempty"`
	Pointer  string `json:"json_pointer,omitempty"`
}
type Retry struct {
	Revision          uint64     `json:"revision"`
	Delays            []Duration `json:"delays"`
	MaxDuration       Duration   `json:"max_duration"`
	RespectRetryAfter bool       `json:"respect_retry_after"`
}
type Quota struct {
	IngressGlobal      int    `json:"ingress_global"`
	IngressClient      int    `json:"ingress_client"`
	EgressTarget       int    `json:"egress_target"`
	EgressClientTarget int    `json:"egress_client_target"`
	GlobalConcurrency  int    `json:"global_concurrency"`
	TargetConcurrency  int    `json:"target_concurrency"`
	FailMode           string `json:"fail_mode"`
}
type Client struct {
	Enabled     bool     `json:"enabled"`
	Targets     []string `json:"targets"`
	ManualRetry bool     `json:"manual_retry"`
	Quota       string   `json:"quota_policy"`
}
type Hook struct {
	Revision   uint64 `json:"revision"`
	Expression string `json:"expression"`
}
type Issuer struct {
	Audience  string            `json:"audience"`
	Algorithm string            `json:"algorithm"`
	Keys      map[string]string `json:"public_keys"`
}
type Snapshot struct {
	Revision uint64
	LoadedAt time.Time
	Document
	Programs map[string]cel.Program
	Keys     map[string]map[string]any
}

func VersionKey(id string, revision uint64) string { return fmt.Sprintf("%s@%d", id, revision) }

var Buckets = []time.Duration{5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute, 30 * time.Minute, 2 * time.Hour, 6 * time.Hour}

func BucketRoute(d time.Duration) string { return fmt.Sprintf("retry.%ds", int64(d/time.Second)) }
