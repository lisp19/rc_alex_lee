package hook

import (
	"context"
	"encoding/json"
	"notifier/internal/config"
	"notifier/internal/domain"
	"reflect"
	"regexp"
	"time"
)

var reasonPattern = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,64}$`)

func Evaluate(ctx context.Context, n *domain.Notification, s *config.Snapshot, d *domain.Decision) {
	t := s.Targets[n.Target]
	program := s.Programs[t.Hook]
	// Transport/security failures are not supplier responses and cannot be
	// overridden by a hook. A complete response hash indicates a bounded read.
	if program == nil || len(d.BodyHash) != 32 {
		return
	}
	var parsed any
	if err := json.Unmarshal(d.Preview, &parsed); err != nil {
		parsed = nil
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	value, _, err := program.ContextEval(ctx, map[string]any{"status": int64(d.HTTPStatus), "headers": d.Headers, "body": string(d.Preview), "json": parsed, "notification": map[string]string{"id": n.ID.String(), "client_id": n.Client, "target_id": n.Target}, "attempt": int64(n.Attempts)})
	invalid := func() { d.Action = "fail"; d.Reason = "hook_evaluation_failed" }
	if err != nil {
		invalid()
		return
	}
	native, err := value.ConvertToNative(reflect.TypeOf(map[string]any{}))
	if err != nil {
		invalid()
		return
	}
	b, err := json.Marshal(native)
	if err != nil || len(b) > 8192 {
		invalid()
		return
	}
	var output struct {
		Action string          `json:"action"`
		Reason string          `json:"reason"`
		Report json.RawMessage `json:"report"`
	}
	if err = config.Decode(b, &output); err != nil {
		invalid()
		return
	}
	if output.Reason != "" && !reasonPattern.MatchString(output.Reason) {
		invalid()
		return
	}
	switch output.Action {
	case "success", "retry", "fail":
		d.Action = output.Action
		if output.Reason != "" {
			d.Reason = output.Reason
		}
	case "report":
	default:
		invalid()
		return
	}
	d.Report = output.Report
}
