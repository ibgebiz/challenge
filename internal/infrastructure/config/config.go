// Package config loads runtime configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds all runtime tunables for the API, worker, and scheduler binaries.
type Config struct {
	HTTPPort              string
	PostgresDSN           string
	RedisAddr             string
	RateLimitPerSec       int
	MaxRetryAttempts      int
	RetryInterval         time.Duration
	SchedulerPollInterval time.Duration
	WorkerConcurrency     int
	ProviderURL           string
	OTELEndpoint          string
	LogLevel              string
}

// Load reads configuration from the environment, applying defaults. It returns
// an error when a required value (Postgres DSN, Redis address) is missing.
func Load() (Config, error) {
	c := Config{
		HTTPPort:              getEnv("HTTP_PORT", "8080"),
		PostgresDSN:           os.Getenv("POSTGRES_DSN"),
		RedisAddr:             os.Getenv("REDIS_ADDR"),
		RateLimitPerSec:       getInt("RATE_LIMIT_PER_SEC", 100),
		MaxRetryAttempts:      getInt("MAX_RETRY_ATTEMPTS", 5),
		RetryInterval:         getDur("RETRY_INTERVAL", 30*time.Second),
		SchedulerPollInterval: getDur("SCHEDULER_POLL_INTERVAL", 5*time.Second),
		WorkerConcurrency:     getInt("WORKER_CONCURRENCY", 10),
		ProviderURL:           os.Getenv("PROVIDER_URL"),
		OTELEndpoint:          os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		LogLevel:              getEnv("LOG_LEVEL", "info"),
	}
	if c.PostgresDSN == "" {
		return c, fmt.Errorf("POSTGRES_DSN required")
	}
	if c.RedisAddr == "" {
		return c, fmt.Errorf("REDIS_ADDR required")
	}
	return c, nil
}

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func getInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getDur(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
