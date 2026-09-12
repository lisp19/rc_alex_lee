// notify-admin validates offline by default. apply is the only control DB writer.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"notifier/internal/config"
	"notifier/internal/repository"
	"os"
	"time"
)

func main() {
	if err := run(); err != nil {
		slog.Error("admin_failed", "error", err)
		os.Exit(1)
	}
}
func run() error {
	file := flag.String("file", "configs/management.example.json", "management JSON")
	apply := flag.Bool("apply", false, "write validated config to NOTIFIER_ADMIN_DSN")
	actor := flag.String("actor", "", "audit actor required with -apply")
	flag.Parse()
	b, err := os.ReadFile(*file)
	if err != nil {
		return err
	}
	s, err := config.Build(0, b)
	if err != nil {
		return err
	}
	if !*apply {
		fmt.Println("configuration valid")
		return nil
	}
	if *actor == "" || len(*actor) > 128 {
		return fmt.Errorf("valid actor required")
	}
	dsn := os.Getenv("NOTIFIER_ADMIN_DSN")
	if dsn == "" {
		return fmt.Errorf("NOTIFIER_ADMIN_DSN required")
	}
	db, err := repository.Open(dsn, 2)
	if err != nil {
		return err
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var rev uint64
	if err = tx.QueryRowContext(ctx, "SELECT revision FROM config_revision WHERE scope='global' FOR UPDATE").Scan(&rev); err != nil {
		return err
	}
	if rev > 0 {
		var previous []byte
		if err = tx.QueryRowContext(ctx, "SELECT document FROM config_snapshot WHERE revision=?", rev).Scan(&previous); err != nil {
			return err
		}
		old, err := config.Build(rev, previous)
		if err != nil {
			return err
		}
		if err = monotonic(old, s); err != nil {
			return err
		}
	}
	rev++
	if _, err = tx.ExecContext(ctx, "INSERT INTO config_snapshot(revision,document,created_at,actor) VALUES(?,?,UTC_TIMESTAMP(6),?)", rev, b, *actor); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE config_revision SET revision=?,updated_at=UTC_TIMESTAMP(6) WHERE scope='global'", rev); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	slog.Info("config_applied", "actor", *actor, "config_revision", rev)
	return nil
}
func monotonic(old, next *config.Snapshot) error {
	check := func(name string, a, b any, ar, br uint64) error {
		ab, _ := json.Marshal(a)
		bb, _ := json.Marshal(b)
		if br < ar || (string(ab) != string(bb) && br <= ar) {
			return fmt.Errorf("%s changed without increasing revision", name)
		}
		return nil
	}
	for id, b := range next.Targets {
		if a, ok := old.Targets[id]; ok {
			if err := check(id, a, b, a.Revision, b.Revision); err != nil {
				return err
			}
		}
	}
	for id, b := range next.Retries {
		if a, ok := old.Retries[id]; ok {
			if err := check(id, a, b, a.Revision, b.Revision); err != nil {
				return err
			}
		}
	}
	for id, b := range next.Hooks {
		if a, ok := old.Hooks[id]; ok {
			if err := check(id, a, b, a.Revision, b.Revision); err != nil {
				return err
			}
		}
	}
	return nil
}
