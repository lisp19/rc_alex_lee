package lock

import (
	"context"
	"github.com/redis/go-redis/v9"
	"log/slog"
	"notifier/internal/domain"
	"time"
)

var unlock = redis.NewScript(`if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) end; return 0`)

type Redis struct{ Client *redis.Client }

func (l *Redis) Acquire(ctx context.Context, id domain.ID) (func(), bool, error) {
	key := "notify:lock:" + id.String()
	token := domain.NewID().String()
	c, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	ok, err := l.Client.SetNX(c, key, token, 60*time.Second).Result()
	if err != nil || !ok {
		return nil, ok, err
	}
	return func() {
		c, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		if err := unlock.Run(c, l.Client, []string{key}, token).Err(); err != nil {
			slog.Debug("lock_release_deferred")
		}
	}, true, nil
}
