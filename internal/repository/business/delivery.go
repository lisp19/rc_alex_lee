package business

import (
	"context"
	"encoding/json"
	"notifier/internal/domain"
	"time"
)

const LeaseTTL = 60 * time.Second

// Claim records a started attempt in the same transaction as the fenced lease.
// No database lock survives the return from this function.
func (s *Store) Claim(ctx context.Context, id domain.ID, generation uint64) (*domain.Notification, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	token := domain.NewID()
	r, err := tx.ExecContext(ctx, `UPDATE notification_task SET status='in_flight',lease_token=?,lease_until=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 60 SECOND),attempt_count=attempt_count+1,cycle_attempt_count=cycle_attempt_count+1,updated_at=UTC_TIMESTAMP(6) WHERE id=? AND dispatch_generation=? AND status='pending' AND next_attempt_at<=UTC_TIMESTAMP(6)`, token[:], id[:], generation)
	if err != nil {
		return nil, err
	}
	affected, err := r.RowsAffected()
	if err != nil {
		return nil, err
	}
	if affected != 1 {
		return nil, domain.ErrState
	}
	n, err := scan(tx.QueryRowContext(ctx, "SELECT "+columns+" FROM notification_task WHERE id=?", id[:]))
	if err != nil {
		return nil, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO delivery_attempt(notification_id,attempt_no,dispatch_generation,lease_token,started_at,result) VALUES(?,?,?,?,UTC_TIMESTAMP(6),'started')`, id[:], n.Attempts, n.Generation, token[:])
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return n, nil
}

// Finish is fenced by token, generation, state AND database lease time. A late
// result cannot overwrite a recovered attempt, even before a new claim occurs.
func (s *Store) Finish(ctx context.Context, n *domain.Notification, d domain.Decision, next time.Time, route string) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	status := "failed"
	generation := n.Generation
	if d.Action == "success" {
		status = "delivered"
	} else if d.Action == "retry" {
		status = "pending"
		generation++
	}
	if next.IsZero() {
		next = n.Next
	}
	r, err := tx.ExecContext(ctx, `UPDATE notification_task SET status=?,dispatch_generation=?,next_attempt_at=?,lease_token=NULL,lease_until=NULL,last_http_status=?,last_error_code=?,updated_at=UTC_TIMESTAMP(6),delivered_at=IF(?='delivered',UTC_TIMESTAMP(6),NULL) WHERE id=? AND dispatch_generation=? AND lease_token=? AND status='in_flight' AND lease_until>UTC_TIMESTAMP(6)`, status, generation, next, d.HTTPStatus, d.Reason, status, n.ID[:], n.Generation, n.Lease[:])
	if err != nil {
		return err
	}
	count, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return domain.ErrState
	}
	headers, err := json.Marshal(d.Headers)
	if err != nil {
		return err
	}
	var report any
	if len(d.Report) > 0 {
		report = []byte(d.Report)
	}
	_, err = tx.ExecContext(ctx, `UPDATE delivery_attempt SET finished_at=UTC_TIMESTAMP(6),http_status=?,result=?,error_code=?,latency_ms=?,response_headers_json=?,response_body_preview=?,response_body_hash=?,hook_result_json=? WHERE notification_id=? AND attempt_no=? AND lease_token=?`, d.HTTPStatus, d.Action, d.Reason, max(0, d.Latency.Milliseconds()), headers, d.Preview, d.BodyHash, report, n.ID[:], n.Attempts, n.Lease[:])
	if err != nil {
		return err
	}
	if status == "pending" {
		copy := *n
		copy.Generation = generation
		copy.Next = next
		if err = insertEvent(ctx, tx, &copy, route, time.Now().UTC()); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// DeferPending handles resource exhaustion without consuming an HTTP attempt.
func (s *Store) DeferPending(ctx context.Context, n *domain.Notification, delay time.Duration) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	next := time.Now().UTC().Add(delay)
	r, err := tx.ExecContext(ctx, `UPDATE notification_task SET dispatch_generation=dispatch_generation+1,next_attempt_at=?,updated_at=UTC_TIMESTAMP(6) WHERE id=? AND dispatch_generation=? AND status='pending'`, next, n.ID[:], n.Generation)
	if err != nil {
		return err
	}
	count, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return domain.ErrState
	}
	copy := *n
	copy.Generation++
	copy.Next = next
	if err = insertEvent(ctx, tx, &copy, "dispatch", next); err != nil {
		return err
	}
	return tx.Commit()
}
