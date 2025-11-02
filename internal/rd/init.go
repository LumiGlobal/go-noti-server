package rd

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/newrelic/go-agent/v3/integrations/nrredis-v9"
	"github.com/redis/go-redis/v9"
)

var (
	Client *redis.Client
)

func Init() {
	opts := &redis.Options{
		Addr:         os.Getenv("REDIS_ADDR"),
		PoolSize:     2,
		MinIdleConns: 1,
		ReadTimeout:  -1,
		WriteTimeout: 5 * time.Second,
		DialTimeout:  5 * time.Second,
	}
	Client = redis.NewClient(opts)
	Client.AddHook(nrredis.NewHook(opts))

	_, err := Client.Ping(context.Background()).Result()
	if err != nil {
		log.Fatalf("Failed to connect to redis: %v\n", err)
	}
}
