package domain

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrNotFound = errors.New("not found")
	ErrConflict = errors.New("idempotency conflict")
	ErrState    = errors.New("invalid state or stale lease")
)

type ID [16]byte

func NewID() ID {
	var id ID
	if _, err := rand.Read(id[:]); err != nil {
		panic(err)
	}
	ms := uint64(time.Now().UnixMilli())
	for i := 5; i >= 0; i-- {
		id[i] = byte(ms)
		ms >>= 8
	}
	id[6] = id[6]&0x0f | 0x70
	id[8] = id[8]&0x3f | 0x80
	return id
}
func ParseID(s string) (ID, error) {
	var id ID
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return id, errors.New("invalid UUID")
	}
	b, err := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	if err != nil || len(b) != 16 {
		return id, errors.New("invalid UUID")
	}
	copy(id[:], b)
	return id, nil
}
func (id ID) String() string {
	s := hex.EncodeToString(id[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s", s[:8], s[8:12], s[12:16], s[16:20], s[20:])
}
func (id ID) MarshalJSON() ([]byte, error) { return json.Marshal(id.String()) }
func (id *ID) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := ParseID(s)
	*id = v
	return err
}

type Request struct {
	Method       string            `json:"method"`
	Path         string            `json:"path"`
	Query        map[string]string `json:"query,omitempty"`
	Headers      map[string]string `json:"headers,omitempty"`
	BodyEncoding string            `json:"body_encoding,omitempty"`
	Body         string            `json:"body,omitempty"`
}
type Submission struct {
	Key     string  `json:"idempotency_key,omitempty"`
	Target  string  `json:"target"`
	Mode    string  `json:"delivery_mode,omitempty"`
	Request Request `json:"request"`
}
type Notification struct {
	ID             ID        `json:"notification_id"`
	Client         string    `json:"-"`
	Target         string    `json:"target"`
	Batch          *ID       `json:"batch_id,omitempty"`
	Key            string    `json:"-"`
	Hash           [32]byte  `json:"-"`
	Request        Request   `json:"-"`
	Mode           string    `json:"delivery_mode"`
	Status         string    `json:"status"`
	Attempts       int       `json:"attempt_count"`
	CycleAttempts  int       `json:"-"`
	Generation     uint64    `json:"dispatch_generation"`
	Next           time.Time `json:"next_attempt_at"`
	Deadline       time.Time `json:"retry_deadline_at"`
	Lease          ID        `json:"-"`
	LeaseUntil     time.Time `json:"-"`
	TargetRevision uint64    `json:"target_revision"`
	RetryRevision  uint64    `json:"retry_revision"`
	HookRevision   uint64    `json:"hook_revision"`
	ConfigRevision uint64    `json:"config_revision"`
	Created        time.Time `json:"accepted_at"`
	Updated        time.Time `json:"updated_at"`
	LastStatus     int       `json:"last_http_status,omitempty"`
	LastError      string    `json:"last_error_code,omitempty"`
}
type Event struct {
	Schema       int       `json:"schema_version"`
	ID           ID        `json:"event_id"`
	Notification ID        `json:"notification_id"`
	Generation   uint64    `json:"generation"`
	Type         string    `json:"event_type"`
	Created      time.Time `json:"created_at"`
}
type Outbox struct {
	ID    int64
	Event Event
	Route string
	Token ID
}
type Decision struct {
	Action     string            `json:"action"`
	Reason     string            `json:"reason,omitempty"`
	HTTPStatus int               `json:"http_status,omitempty"`
	Headers    map[string]string `json:"headers,omitempty"`
	Preview    []byte            `json:"body_preview,omitempty"`
	BodyHash   []byte            `json:"-"`
	Report     json.RawMessage   `json:"report,omitempty"`
	RetryAfter string            `json:"-"`
	Uncertain  bool              `json:"-"`
	Started    time.Time         `json:"-"`
	Latency    time.Duration     `json:"-"`
}
