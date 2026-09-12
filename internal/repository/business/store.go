package business

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/go-sql-driver/mysql"
	"notifier/internal/domain"
)

type Store struct{ DB *sql.DB }
type scanner interface{ Scan(...any) error }

const columns = `id,client_id,target_id,batch_id,client_idem_key,request_hash,request_json,delivery_mode,status,attempt_count,cycle_attempt_count,dispatch_generation,next_attempt_at,retry_deadline_at,lease_token,lease_until,target_revision,retry_revision,hook_revision,config_revision,created_at,updated_at,last_http_status,last_error_code`

func scan(row scanner) (*domain.Notification, error) {
	n := new(domain.Notification)
	var id, batch, hash, lease, request []byte
	var until sql.NullTime
	err := row.Scan(&id, &n.Client, &n.Target, &batch, &n.Key, &hash, &request, &n.Mode, &n.Status, &n.Attempts, &n.CycleAttempts, &n.Generation, &n.Next, &n.Deadline, &lease, &until, &n.TargetRevision, &n.RetryRevision, &n.HookRevision, &n.ConfigRevision, &n.Created, &n.Updated, &n.LastStatus, &n.LastError)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	copy(n.ID[:], id)
	copy(n.Hash[:], hash)
	copy(n.Lease[:], lease)
	n.LeaseUntil = until.Time
	if len(batch) == 16 {
		b := new(domain.ID)
		copy(b[:], batch)
		n.Batch = b
	}
	if err = json.Unmarshal(request, &n.Request); err != nil {
		return nil, err
	}
	return n, nil
}
func (s *Store) Get(ctx context.Context, id domain.ID, client string) (*domain.Notification, error) {
	return scan(s.DB.QueryRowContext(ctx, "SELECT "+columns+" FROM notification_task WHERE id=? AND client_id=?", id[:], client))
}
func (s *Store) Load(ctx context.Context, id domain.ID) (*domain.Notification, error) {
	return scan(s.DB.QueryRowContext(ctx, "SELECT "+columns+" FROM notification_task WHERE id=?", id[:]))
}
func (s *Store) ByKey(ctx context.Context, client, key string) (*domain.Notification, error) {
	return scan(s.DB.QueryRowContext(ctx, "SELECT "+columns+" FROM notification_task WHERE client_id=? AND client_idem_key=?", client, key))
}
func (s *Store) Accept(ctx context.Context, n *domain.Notification) (*domain.Notification, bool, error) {
	request, err := json.Marshal(n.Request)
	if err != nil {
		return nil, false, err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback()
	var batch any
	if n.Batch != nil {
		batch = n.Batch[:]
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO notification_task
 (id,client_id,target_id,batch_id,client_idem_key,request_hash,request_json,delivery_mode,status,dispatch_generation,next_attempt_at,retry_deadline_at,target_revision,retry_revision,hook_revision,config_revision,created_at,updated_at)
 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, n.ID[:], n.Client, n.Target, batch, n.Key, n.Hash[:], request, n.Mode, n.Status, n.Generation, n.Next, n.Deadline, n.TargetRevision, n.RetryRevision, n.HookRevision, n.ConfigRevision, n.Created, n.Updated)
	if err != nil {
		var me *mysql.MySQLError
		if !errors.As(err, &me) || me.Number != 1062 {
			return nil, false, err
		}
		if err = tx.Rollback(); err != nil {
			return nil, false, err
		}
		old, err := s.ByKey(ctx, n.Client, n.Key)
		if err != nil {
			return nil, false, err
		}
		if old.Hash != n.Hash {
			return nil, false, domain.ErrConflict
		}
		return old, false, nil
	}
	if err = insertEvent(ctx, tx, n, "dispatch", n.Next); err != nil {
		return nil, false, err
	}
	if err = tx.Commit(); err != nil {
		return nil, false, err
	}
	return n, true, nil
}
func insertEvent(ctx context.Context, tx *sql.Tx, n *domain.Notification, route string, available time.Time) error {
	e := domain.Event{Schema: 1, ID: domain.NewID(), Notification: n.ID, Generation: n.Generation, Type: "dispatch", Created: time.Now().UTC()}
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO mq_outbox(event_id,notification_id,generation,event_type,routing_key,available_at,payload_json,status,created_at) VALUES(?,?,?,?,?,?,?,'pending',UTC_TIMESTAMP(6))`, e.ID[:], n.ID[:], n.Generation, e.Type, route, available, b)
	return err
}
func (s *Store) CreateBatch(ctx context.Context, id domain.ID, client string, count int) error {
	_, err := s.DB.ExecContext(ctx, "INSERT INTO notification_batch(id,client_id,item_count,created_at) VALUES(?,?,?,UTC_TIMESTAMP(6))", id[:], client, count)
	return err
}

type Filter struct {
	Status, Target string
	Batch          *domain.ID
	Cursor         *domain.ID
	Limit          int
}

func (s *Store) List(ctx context.Context, client string, f Filter) ([]*domain.Notification, error) {
	q := "SELECT " + columns + " FROM notification_task WHERE client_id=?"
	args := []any{client}
	if f.Status != "" {
		q += " AND status=?"
		args = append(args, f.Status)
	}
	if f.Target != "" {
		q += " AND target_id=?"
		args = append(args, f.Target)
	}
	if f.Batch != nil {
		q += " AND batch_id=?"
		args = append(args, f.Batch[:])
	}
	if f.Cursor != nil {
		q += " AND id<?"
		args = append(args, f.Cursor[:])
	}
	q += " ORDER BY id DESC LIMIT ?"
	args = append(args, f.Limit)
	rows, err := s.DB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]*domain.Notification, 0)
	for rows.Next() {
		n, err := scan(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, n)
	}
	return result, rows.Err()
}

type Attempt struct {
	Number     int             `json:"attempt_no"`
	Generation uint64          `json:"generation"`
	Started    time.Time       `json:"started_at"`
	Finished   *time.Time      `json:"finished_at,omitempty"`
	Status     int             `json:"http_status"`
	Result     string          `json:"result"`
	Error      string          `json:"error_code,omitempty"`
	Latency    int64           `json:"latency_ms"`
	Report     json.RawMessage `json:"report,omitempty"`
}

func (s *Store) Attempts(ctx context.Context, id domain.ID) ([]Attempt, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT attempt_no,dispatch_generation,started_at,finished_at,http_status,result,error_code,latency_ms,hook_result_json FROM delivery_attempt WHERE notification_id=? ORDER BY attempt_no DESC LIMIT 100`, id[:])
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]Attempt, 0)
	for rows.Next() {
		var a Attempt
		if err = rows.Scan(&a.Number, &a.Generation, &a.Started, &a.Finished, &a.Status, &a.Result, &a.Error, &a.Latency, &a.Report); err != nil {
			return nil, err
		}
		items = append(items, a)
	}
	return items, rows.Err()
}
