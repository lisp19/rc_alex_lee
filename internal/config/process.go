package config

import (
	"errors"
	"os"
	"strings"
)

type Process struct {
	Listen            string                  `json:"listen"`
	HealthListen      string                  `json:"health_listen"`
	Roles             []string                `json:"roles"`
	Workers           int                     `json:"workers"`
	Prefetch          int                     `json:"prefetch"`
	DBMaxOpen         int                     `json:"db_max_open"`
	TargetConcurrency int                     `json:"target_concurrency"`
	Secrets           map[string]SecretSource `json:"secrets"`
	BusinessDSN       string                  `json:"-"`
	ControlDSN        string                  `json:"-"`
	AMQPURL           string                  `json:"-"`
	RedisURL          string                  `json:"-"`
}
type SecretSource struct {
	File string `json:"file,omitempty"`
	Env  string `json:"env,omitempty"`
}

func LoadProcess(path string) (Process, error) {
	c := Process{Listen: ":8080", HealthListen: "127.0.0.1:8081", Roles: []string{"api", "worker", "outbox", "recovery"}, Workers: 32, Prefetch: 32, DBMaxOpen: 64, TargetConcurrency: 8}
	b, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	if err = Decode(b, &c); err != nil {
		return c, err
	}
	if v := os.Getenv("NOTIFIER_ROLES"); v != "" {
		c.Roles = strings.Split(v, ",")
	}
	c.BusinessDSN = os.Getenv("NOTIFIER_BUSINESS_DSN")
	c.ControlDSN = os.Getenv("NOTIFIER_CONTROL_DSN")
	c.AMQPURL = os.Getenv("NOTIFIER_AMQP_URL")
	c.RedisURL = os.Getenv("NOTIFIER_REDIS_URL")
	if c.Workers < 1 || c.Workers > 1024 || c.Prefetch < 1 || c.Prefetch > 4096 || c.DBMaxOpen < 2 || c.TargetConcurrency < 1 || len(c.Roles) == 0 {
		return c, errors.New("invalid process limits")
	}
	seen := map[string]bool{}
	for _, r := range c.Roles {
		if seen[r] || (r != "api" && r != "worker" && r != "outbox" && r != "recovery") {
			return c, errors.New("invalid roles")
		}
		seen[r] = true
	}
	if c.BusinessDSN == "" || c.ControlDSN == "" || c.RedisURL == "" {
		return c, errors.New("database/Redis environment variables required")
	}
	if (seen["worker"] || seen["outbox"]) && c.AMQPURL == "" {
		return c, errors.New("AMQP URL required")
	}
	return c, nil
}
