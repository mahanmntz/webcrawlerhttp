package config

import (
	"os"
	"strconv"
	"time"
)

// Config encapsulates runtime settings for the crawler engine.
type Config struct {
	RedisAddr         string
	WorkerCount       int
	FetchTimeout      time.Duration
	PolitenessDelayMs int
	UserAgent         string
}

// Load reads configuration from the environment with production-safe defaults.
func Load() *Config {
	return &Config{
		RedisAddr:         getEnv("REDIS_ADDR", "localhost:6379"),
		WorkerCount:       getEnvAsInt("WORKER_COUNT", 5),
		FetchTimeout:      getEnvAsDuration("FETCH_TIMEOUT_SEC", 10*time.Second),
		PolitenessDelayMs: getEnvAsInt("POLITENESS_DELAY_MS", 1000),
		UserAgent:         getEnv("USER_AGENT", "DistributedEnterpriseCrawler/1.0 (+https://example.com/bot)"),
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvAsInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}

func getEnvAsDuration(key string, defaultVal time.Duration) time.Duration {
	if val := os.Getenv(key); val != "" {
		if sec, err := strconv.Atoi(val); err == nil {
			return time.Duration(sec) * time.Second
		}
	}
	return defaultVal
}
