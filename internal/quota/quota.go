package quota

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"log/slog"
	"notifier/internal/config"
	"notifier/internal/domain"
	"sync"
	"sync/atomic"
	"time"
)

// All dimensions are checked before any is incremented. Redis server time
// keeps fixed one-second windows consistent across pods. Keys share a hash tag.
var rateScript = redis.NewScript(`
local now=redis.call('TIME'); local window=now[1]
for i,k in ipairs(KEYS) do
 local v=redis.call('HMGET',k,'window','count')
 if tonumber(ARGV[i])>0 and v[1]==window and tonumber(v[2] or '0')>=tonumber(ARGV[i]) then return 0 end
end
for i,k in ipairs(KEYS) do
 if tonumber(ARGV[i])>0 then
  local old=redis.call('HGET',k,'window')
  if old~=window then redis.call('HSET',k,'window',window,'count',0) end
  redis.call('HINCRBY',k,'count',1); redis.call('PEXPIRE',k,2000)
 end
end
return 1`)

var egressScript = redis.NewScript(`
local clock=redis.call('TIME'); local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)
for i=1,2 do
 local v=redis.call('HMGET',KEYS[i],'window','count')
 if tonumber(ARGV[i])>0 and v[1]==clock[1] and tonumber(v[2] or '0')>=tonumber(ARGV[i]) then return 0 end
end
for i=3,4 do
 redis.call('ZREMRANGEBYSCORE',KEYS[i],'-inf',now)
 if tonumber(ARGV[i])>0 and redis.call('ZCARD',KEYS[i])>=tonumber(ARGV[i]) then return 0 end
end
for i=1,2 do
 if tonumber(ARGV[i])>0 then
  if redis.call('HGET',KEYS[i],'window')~=clock[1] then redis.call('HSET',KEYS[i],'window',clock[1],'count',0) end
  redis.call('HINCRBY',KEYS[i],'count',1); redis.call('PEXPIRE',KEYS[i],2000)
 end
end
for i=3,4 do
 if tonumber(ARGV[i])>0 then redis.call('ZADD',KEYS[i],now+60000,ARGV[5]); redis.call('PEXPIRE',KEYS[i],65000) end
end
return 1`)
var releaseScript = redis.NewScript(`for _,k in ipairs(KEYS) do redis.call('ZREM',k,ARGV[1]) end; return 1`)

type localTarget struct{ count int }
type Limiter struct {
	Redis                *redis.Client
	MaxGlobal, MaxTarget int
	mu                   sync.Mutex
	active               int
	targets              map[string]*localTarget
	Rejected             atomic.Uint64
	Degraded             atomic.Bool
}

func New(client *redis.Client, global, target int) *Limiter {
	return &Limiter{Redis: client, MaxGlobal: global, MaxTarget: target, targets: map[string]*localTarget{}}
}
func (l *Limiter) Ingress(ctx context.Context, client string, q config.Quota) (bool, error) {
	c, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	n, err := rateScript.Run(c, l.Redis, []string{"notify:{quota}:ingress:global", "notify:{quota}:ingress:client:" + client}, q.IngressGlobal, q.IngressClient).Int()
	if err != nil {
		l.Degraded.Store(true)
		if q.FailMode == "open" {
			return true, nil
		}
		return false, err
	}
	l.Degraded.Store(false)
	if n == 0 {
		l.Rejected.Add(1)
	}
	return n == 1, nil
}
func (l *Limiter) local(target string) (func(), bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	t := l.targets[target]
	if t == nil {
		t = &localTarget{}
		l.targets[target] = t
	}
	if l.active >= l.MaxGlobal || t.count >= l.MaxTarget {
		if t.count == 0 {
			delete(l.targets, target)
		}
		return nil, false
	}
	l.active++
	t.count++
	return func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.active--
		t.count--
		if t.count == 0 {
			delete(l.targets, target)
		}
	}, true
}
func (l *Limiter) Acquire(ctx context.Context, n *domain.Notification, q config.Quota) (func(), bool, error) {
	localRelease, ok := l.local(n.Target)
	if !ok {
		l.Rejected.Add(1)
		return nil, false, nil
	}
	keys := []string{"notify:{quota}:egress:target:" + n.Target, "notify:{quota}:egress:client_target:" + n.Client + ":" + n.Target, "notify:{quota}:concurrency:global", "notify:{quota}:concurrency:target:" + n.Target}
	token := domain.NewID().String()
	c, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	allowed, err := egressScript.Run(c, l.Redis, keys, q.EgressTarget, q.EgressClientTarget, q.GlobalConcurrency, q.TargetConcurrency, token).Int()
	cancel()
	if err != nil {
		l.Degraded.Store(true)
		if q.FailMode == "open" {
			return localRelease, true, nil
		}
		localRelease()
		return nil, false, fmt.Errorf("egress quota unavailable: %w", err)
	}
	l.Degraded.Store(false)
	if allowed != 1 {
		localRelease()
		l.Rejected.Add(1)
		return nil, false, nil
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			defer localRelease()
			c, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			if err := releaseScript.Run(c, l.Redis, keys[2:], token).Err(); err != nil {
				slog.Warn("quota_release_deferred")
			}
		})
	}
	return release, true, nil
}
