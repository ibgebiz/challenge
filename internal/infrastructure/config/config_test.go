package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "dsn")
	t.Setenv("REDIS_ADDR", "localhost:6379")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.RateLimitPerSec != 100 {
		t.Fatalf("want 100, got %d", cfg.RateLimitPerSec)
	}
	if cfg.MaxRetryAttempts != 5 {
		t.Fatalf("want 5, got %d", cfg.MaxRetryAttempts)
	}
	if cfg.RetryInterval != 30*time.Second {
		t.Fatalf("want 30s, got %v", cfg.RetryInterval)
	}
	if cfg.HTTPPort != "8080" {
		t.Fatalf("want 8080, got %s", cfg.HTTPPort)
	}
}

func TestLoadRequiresDSN(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "")
	t.Setenv("REDIS_ADDR", "localhost:6379")
	if _, err := Load(); err == nil {
		t.Fatal("expected error when POSTGRES_DSN missing")
	}
}

func TestLoadOverrides(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "dsn")
	t.Setenv("REDIS_ADDR", "localhost:6379")
	t.Setenv("RATE_LIMIT_PER_SEC", "250")
	t.Setenv("RETRY_INTERVAL", "5s")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.RateLimitPerSec != 250 {
		t.Fatalf("want 250, got %d", cfg.RateLimitPerSec)
	}
	if cfg.RetryInterval != 5*time.Second {
		t.Fatalf("want 5s, got %v", cfg.RetryInterval)
	}
}
