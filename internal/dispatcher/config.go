package dispatcher

import (
	"os"
	"strconv"
	"time"
)

// Config holds all operational parameters for the webhook dispatcher engine.
type Config struct {
	ListenAddr          string
	DBPath              string
	WorkerCount         int
	QueueCapacity       int
	MaxRetries          int
	BaseBackoff         time.Duration
	MaxBackoff          time.Duration
	DeliveryTimeout     time.Duration
	ShutdownTimeout     time.Duration
	DefaultSecret       string
	RetryAfterSeconds   int
	MaxBatchSize        int
	BatchCoalesceWindow time.Duration
}

// DefaultConfig returns production-tuned default settings.
func DefaultConfig() Config {
	return Config{
		ListenAddr:          envString("DISPATCHER_ADDR", ":8080"),
		DBPath:              envString("DISPATCHER_DB_PATH", "./dispatcher.db"),
		WorkerCount:         envInt("DISPATCHER_WORKERS", 64),
		QueueCapacity:       envInt("DISPATCHER_QUEUE_CAPACITY", 4096),
		MaxRetries:          envInt("DISPATCHER_MAX_RETRIES", 3),
		BaseBackoff:         envDuration("DISPATCHER_BASE_BACKOFF", 50*time.Millisecond),
		MaxBackoff:          envDuration("DISPATCHER_MAX_BACKOFF", 2*time.Second),
		DeliveryTimeout:     envDuration("DISPATCHER_DELIVERY_TIMEOUT", 5*time.Second),
		ShutdownTimeout:     envDuration("DISPATCHER_SHUTDOWN_TIMEOUT", 15*time.Second),
		DefaultSecret:       envString("DISPATCHER_SIGNING_SECRET", "whsec_prod_9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"),
		RetryAfterSeconds:   envInt("DISPATCHER_RETRY_AFTER_SEC", 1),
		MaxBatchSize:        envInt("DISPATCHER_MAX_BATCH_SIZE", 128),
		BatchCoalesceWindow: envDuration("DISPATCHER_BATCH_WINDOW", 250*time.Microsecond),
	}
}

func envString(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return fallback
}
