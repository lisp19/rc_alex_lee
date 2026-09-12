package config

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
)

type Manager struct {
	DB       *sql.DB
	current  atomic.Pointer[Snapshot]
	secrets  atomic.Pointer[map[string]string]
	mu       sync.Mutex
	history  map[uint64]*Snapshot
	Reloads  atomic.Uint64
	Failures atomic.Uint64
}

func NewManager(db *sql.DB) *Manager  { return &Manager{DB: db, history: map[uint64]*Snapshot{}} }
func (m *Manager) Current() *Snapshot { return m.current.Load() }
func (m *Manager) Secret(ref string) (string, error) {
	p := m.secrets.Load()
	if p == nil {
		return "", errors.New("secrets unavailable")
	}
	v, ok := (*p)[ref]
	if !ok {
		return "", errors.New("secret unavailable")
	}
	return v, nil
}
func (m *Manager) Reload(ctx context.Context) error {
	var rev uint64
	if err := m.DB.QueryRowContext(ctx, "SELECT revision FROM config_revision WHERE scope='global'").Scan(&rev); err != nil {
		return err
	}
	if old := m.Current(); old != nil && old.Revision == rev {
		return nil
	}
	s, err := m.History(ctx, rev)
	if err != nil {
		return err
	}
	for _, t := range s.Targets {
		if t.Enabled && t.Auth.Type == "static_header" {
			if _, err := m.Secret(t.Auth.SecretRef); err != nil {
				return err
			}
		}
	}
	m.current.Store(s)
	m.Reloads.Add(1)
	slog.Info("config_reloaded", "config_revision", rev)
	return nil
}
func (m *Manager) History(ctx context.Context, rev uint64) (*Snapshot, error) {
	m.mu.Lock()
	s := m.history[rev]
	m.mu.Unlock()
	if s != nil {
		return s, nil
	}
	var b []byte
	if err := m.DB.QueryRowContext(ctx, "SELECT document FROM config_snapshot WHERE revision=?", rev).Scan(&b); err != nil {
		return nil, err
	}
	s, err := Build(rev, b)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// Bound historical cache; active pointers remain valid after eviction.
	if len(m.history) >= 64 {
		for k := range m.history {
			delete(m.history, k)
			break
		}
	}
	m.history[rev] = s
	return s, nil
}
func (m *Manager) LoadSecrets(sources map[string]SecretSource) ([32]byte, error) {
	values := map[string]string{}
	for id, s := range sources {
		if (s.File == "") == (s.Env == "") {
			return [32]byte{}, errors.New("secret must use exactly one file or env")
		}
		v := ""
		if s.File != "" {
			b, err := os.ReadFile(s.File)
			if err != nil {
				return [32]byte{}, errors.New("cannot read secret file")
			}
			v = strings.TrimSpace(string(b))
		} else {
			v = os.Getenv(s.Env)
		}
		if v == "" || len(v) > 16384 || strings.ContainsAny(v, "\r\n\x00") {
			return [32]byte{}, errors.New("empty or invalid secret")
		}
		values[id] = v
	}
	if current := m.Current(); current != nil {
		for _, t := range current.Targets {
			if t.Enabled && t.Auth.Type == "static_header" && values[t.Auth.SecretRef] == "" {
				return [32]byte{}, errors.New("active secret reference missing")
			}
		}
	}
	m.secrets.Store(&values)
	// Hash only used for change detection, never emitted.
	var joined strings.Builder
	for id := range sources {
		joined.WriteString(id)
		joined.WriteString(values[id])
	}
	return sha256.Sum256([]byte(joined.String())), nil
}
func (m *Manager) Watch(ctx context.Context, path string, p Process) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		slog.Error("file_watcher_failed")
		return
	}
	defer w.Close()
	dirs := map[string]bool{filepath.Dir(path): true}
	for _, s := range p.Secrets {
		if s.File != "" {
			dirs[filepath.Dir(s.File)] = true
		}
	}
	for d := range dirs {
		if err := w.Add(d); err != nil {
			slog.Warn("watch_directory_failed", "directory", d)
		}
	}
	dbTick := time.NewTicker(2 * time.Second)
	defer dbTick.Stop()
	fallback := time.NewTicker(30 * time.Second)
	defer fallback.Stop()
	debounce := time.NewTimer(time.Hour)
	if !debounce.Stop() {
		<-debounce.C
	}
	defer debounce.Stop()
	reloadFiles := func() {
		next, err := LoadProcess(path)
		if err == nil {
			_, err = m.LoadSecrets(next.Secrets)
		}
		if err != nil {
			m.Failures.Add(1)
			slog.Error("config_reload_failed", "source", "file")
			return
		}
		for _, s := range next.Secrets {
			if s.File != "" && !dirs[filepath.Dir(s.File)] {
				d := filepath.Dir(s.File)
				if w.Add(d) == nil {
					dirs[d] = true
				}
			}
		}
		m.Reloads.Add(1)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-w.Events:
			if !ok {
				return
			}
			debounce.Reset(300 * time.Millisecond)
		case _, ok := <-w.Errors:
			if !ok {
				return
			}
			m.Failures.Add(1)
		case <-debounce.C:
			reloadFiles()
		case <-fallback.C:
			reloadFiles()
		case <-dbTick.C:
			c, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := m.Reload(c)
			cancel()
			if err != nil {
				m.Failures.Add(1)
				slog.Error("config_reload_failed", "source", "database")
			}
		}
	}
}
