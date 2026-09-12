package business

import (
	"context"
	"notifier/internal/domain"
	"time"
)

func (s *Store) Expired(ctx context.Context) ([]*domain.Notification, error) {
	rows, err := s.DB.QueryContext(ctx, "SELECT "+columns+" FROM notification_task WHERE status='in_flight' AND lease_until<UTC_TIMESTAMP(6) ORDER BY lease_until LIMIT 100")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]*domain.Notification, 0)
	for rows.Next() {
		n, err := scan(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, n)
	}
	return items, rows.Err()
}
func (s *Store) RecoverLease(ctx context.Context, n *domain.Notification, d domain.Decision, next time.Time, route string) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	status := "failed"
	if d.Action == "retry" {
		status = "pending"
	}
	if next.IsZero() {
		next = n.Next
	}
	r, err := tx.ExecContext(ctx, `UPDATE notification_task SET status=?,dispatch_generation=dispatch_generation+1,lease_token=NULL,lease_until=NULL,next_attempt_at=?,last_error_code=?,updated_at=UTC_TIMESTAMP(6) WHERE id=? AND status='in_flight' AND dispatch_generation=? AND lease_token=? AND lease_until<UTC_TIMESTAMP(6)`, status, next, d.Reason, n.ID[:], n.Generation, n.Lease[:])
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
	_, err = tx.ExecContext(ctx, `UPDATE delivery_attempt SET finished_at=UTC_TIMESTAMP(6),result='unknown',error_code='lease_expired' WHERE notification_id=? AND attempt_no=? AND lease_token=? AND finished_at IS NULL`, n.ID[:], n.Attempts, n.Lease[:])
	if err != nil {
		return err
	}
	if status == "pending" {
		copy := *n
		copy.Generation++
		copy.Next = next
		if err = insertEvent(ctx, tx, &copy, route, time.Now().UTC()); err != nil {
			return err
		}
	}
	return tx.Commit()
}
func (s *Store) RecoverOutbox(ctx context.Context) error {
	_, err := s.DB.ExecContext(ctx, `UPDATE mq_outbox SET status='pending',publish_token=NULL,lease_until=NULL WHERE status='publishing' AND lease_until<UTC_TIMESTAMP(6) LIMIT 1000`)
	return err
}

// Reconcile advances generation when a due task has no live publisher and its
// confirmed trigger is stale. A published row is not proof that MQ still holds
// a message. Reconciliation therefore also repairs broker-side data loss.
func (s *Store) Reconcile(ctx context.Context) (int, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT id,dispatch_generation FROM notification_task n
 WHERE status='pending' AND next_attempt_at<DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 60 SECOND)
 AND NOT EXISTS(SELECT 1 FROM mq_outbox o WHERE o.notification_id=n.id AND o.generation=n.dispatch_generation AND (o.status IN ('pending','publishing') OR o.published_at>DATE_SUB(UTC_TIMESTAMP(6),INTERVAL 60 SECOND)))
 ORDER BY next_attempt_at LIMIT 100 FOR UPDATE SKIP LOCKED`)
	if err != nil {
		return 0, err
	}
	var tasks []domain.Notification
	for rows.Next() {
		var b []byte
		var n domain.Notification
		if err = rows.Scan(&b, &n.Generation); err != nil {
			rows.Close()
			return 0, err
		}
		copy(n.ID[:], b)
		tasks = append(tasks, n)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	if err = rows.Close(); err != nil {
		return 0, err
	}
	for _, n := range tasks {
		n.Generation++
		n.Next = time.Now().UTC()
		if _, err = tx.ExecContext(ctx, "UPDATE notification_task SET dispatch_generation=?,updated_at=UTC_TIMESTAMP(6) WHERE id=?", n.Generation, n.ID[:]); err != nil {
			return 0, err
		}
		if err = insertEvent(ctx, tx, &n, "dispatch", n.Next); err != nil {
			return 0, err
		}
	}
	return len(tasks), tx.Commit()
}
func (s *Store) ManualRetry(ctx context.Context, n *domain.Notification, duration time.Duration) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	r, err := tx.ExecContext(ctx, `UPDATE notification_task SET status='pending',dispatch_generation=dispatch_generation+1,cycle_attempt_count=0,next_attempt_at=?,retry_deadline_at=?,last_error_code='',updated_at=UTC_TIMESTAMP(6) WHERE id=? AND client_id=? AND status='failed' AND dispatch_generation=?`, now, now.Add(duration), n.ID[:], n.Client, n.Generation)
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
	copy.Next = now
	if err = insertEvent(ctx, tx, &copy, "dispatch", now); err != nil {
		return err
	}
	return tx.Commit()
}
