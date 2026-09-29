package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"crawler-engine/internal/config"
	"crawler-engine/internal/fetcher"
	"crawler-engine/internal/frontier"
	"crawler-engine/internal/robots"
	"crawler-engine/internal/worker"

	"github.com/redis/go-redis/v9"
)

func main() {
	log.Println("==================================================================")
	log.Println(" Distributed Web Crawler Engine (Go) - Alex Xu Chapter 9 Model ")
	log.Println("==================================================================")

	cfg := config.Load()
	log.Printf("[Config] Redis: %s | Workers: %d | Timeout: %v | Politeness: %dms",
		cfg.RedisAddr, cfg.WorkerCount, cfg.FetchTimeout, cfg.PolitenessDelayMs)

	// Initialize Redis Client
	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     cfg.WorkerCount * 2,
	})

	// Healthcheck Redis
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer pingCancel()

	if err := rdb.Ping(pingCtx).Err(); err != nil {
		log.Fatalf("[FATAL] Could not connect to Redis at %s: %v", cfg.RedisAddr, err)
	}
	log.Println("[Init] Successfully connected to Redis broker.")

	// Construct Components
	frontierClient := frontier.New(rdb, cfg.VisibilityTimeout, cfg.MaxRedeliveries)
	fetcherClient := fetcher.New(cfg)

	var robotsChecker *robots.Checker
	if cfg.RespectRobots {
		robotsChecker = robots.New(rdb, fetcherClient.Client(), fetcherClient.UserAgent(), cfg.RobotsUserAgent)
	} else {
		log.Println("[Config] ⚠️  RESPECT_ROBOTS=false: robots.txt is NOT being enforced")
	}
	if cfg.AllowPrivateNetworks {
		log.Println("[Config] ⚠️  ALLOW_PRIVATE_NETWORKS=true: SSRF guard is disabled")
	}

	workerPool := worker.New(worker.Options{
		WorkerCount:     cfg.WorkerCount,
		MaxAttempts:     cfg.MaxAttempts,
		PolitenessDelay: time.Duration(cfg.PolitenessDelayMs) * time.Millisecond,
		MaxCrawlDelay:   cfg.MaxCrawlDelay,
	}, frontierClient, fetcherClient, robotsChecker)

	// Setup Graceful Shutdown via OS Signals
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Launch Worker Pool
	workerPool.Start(ctx)

	// Wait for shutdown signal
	<-ctx.Done()
	log.Println("\n[Shutdown] Caught shutdown signal. Stopping worker pool gracefully...")

	// Drain and stop workers
	workerPool.Stop()

	// Close Redis connection
	if err := rdb.Close(); err != nil {
		log.Printf("[Shutdown] Error closing Redis connection: %v", err)
	}

	log.Println("[Shutdown] Crawler Engine terminated cleanly. Goodbye!")
}
