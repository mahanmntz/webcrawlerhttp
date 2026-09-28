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

	// MaxBodyBytes caps how much of a response body is read.
	MaxBodyBytes int64
	// AllowPrivateNetworks disables the SSRF guard. Only for local testing.
	AllowPrivateNetworks bool

	// MaxAttempts is how many times a target is fetched before it is
	// dead-lettered on transient failures (network errors, 429, 5xx).
	MaxAttempts int
	// VisibilityTimeout is how long a dequeued target may stay in
	// frontier:processing before the reaper assumes its worker died.
	VisibilityTimeout time.Duration
	// MaxRedeliveries is how many times the reaper re-queues the same target
	// before dead-lettering it (protects against targets that crash workers).
	MaxRedeliveries int

	// RespectRobots enables robots.txt checks and Crawl-delay.
	RespectRobots bool
	// RobotsUserAgent is the product token matched against robots.txt groups.
	RobotsUserAgent string
	// MaxCrawlDelay caps a host's robots.txt Crawl-delay.
	MaxCrawlDelay time.Duration
}

// Load reads configuration from the environment with production-safe defaults.
func Load() *Config {
	return &Config{
		RedisAddr:         getEnv("REDIS_ADDR", "localhost:6379"),
		WorkerCount:       getEnvAsInt("WORKER_COUNT", 5),
		FetchTimeout:      getEnvAsDuration("FETCH_TIMEOUT_SEC", 15*time.Second),
		PolitenessDelayMs: getEnvAsInt("POLITENESS_DELAY_MS", 1000),
		UserAgent:         getEnv("USER_AGENT", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 SpiderRAG/1.0"),

		MaxBodyBytes:         int64(getEnvAsInt("MAX_BODY_BYTES", 5*1024*1024)),
		AllowPrivateNetworks: getEnvAsBool("ALLOW_PRIVATE_NETWORKS", false),

		MaxAttempts:       getEnvAsInt("MAX_ATTEMPTS", 3),
		VisibilityTimeout: getEnvAsDuration("VISIBILITY_TIMEOUT_SEC", 2*time.Minute),
		MaxRedeliveries:   getEnvAsInt("MAX_REDELIVERIES", 3),

		RespectRobots:   getEnvAsBool("RESPECT_ROBOTS", true),
		RobotsUserAgent: getEnv("ROBOTS_USER_AGENT", "SpiderRAG"),
		MaxCrawlDelay:   getEnvAsDuration("MAX_CRAWL_DELAY_SEC", 30*time.Second),
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

func getEnvAsBool(key string, defaultVal bool) bool {
	if val := os.Getenv(key); val != "" {
		if b, err := strconv.ParseBool(val); err == nil {
			return b
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
