package business

import (
	"context"
	"encoding/json"
	"notifier/internal/domain"
)

func (s *Store) ClaimOutbox(ctx context.Context) (*domain.Outbox, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	o := new(domain.Outbox)
	var payload []byte
	err = tx.QueryRowContext(ctx, `SELECT id,payload_json,routing_key FROM mq_outbox WHERE status='pending' AND available_at<=UTC_TIMESTAMP(6) ORDER BY available_at,id LIMIT 1 FOR UPDATE SKIP LOCKED`).Scan(&o.ID, &payload, &o.Route)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(payload, &o.Event); err != nil {
		return nil, err
	}
	o.Token = domain.NewID()
	_, err = tx.ExecContext(ctx, `UPDATE mq_outbox SET status='publishing',publish_token=?,lease_until=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 30 SECOND) WHERE id=?`, o.Token[:], o.ID)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return o, nil
}
func (s *Store) Published(ctx context.Context, o *domain.Outbox) error {
	r, err := s.DB.ExecContext(ctx, `UPDATE mq_outbox SET status='published',published_at=UTC_TIMESTAMP(6),publish_token=NULL,lease_until=NULL WHERE id=? AND status='publishing' AND publish_token=? AND lease_until>UTC_TIMESTAMP(6)`, o.ID, o.Token[:])
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
	return nil
}
func (s *Store) ReleaseOutbox(ctx context.Context, o *domain.Outbox) error {
	_, err := s.DB.ExecContext(ctx, `UPDATE mq_outbox SET status='pending',available_at=DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 2 SECOND),publish_token=NULL,lease_until=NULL WHERE id=? AND status='publishing' AND publish_token=?`, o.ID, o.Token[:])
	return err
}
