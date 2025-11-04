package datastore

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/newrelic/go-agent/v3/integrations/nrredis-v9"
	"github.com/redis/go-redis/v9"
)

var client *redis.Client

const (
	jobPayloadHashSet = "job:payload:hash"
	jobsQueue         = "jobs"
	processingQueue   = "processing"
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
	client = redis.NewClient(opts)
	client.AddHook(nrredis.NewHook(opts))

	_, err := client.Ping(context.Background()).Result()
	if err != nil {
		log.Fatalf("Failed to connect to redis: %v\n", err)
	}
}

func MoveJobToProcessing(ctx context.Context) (string, error) {
	jobId, err := client.BLMove(ctx, jobsQueue, processingQueue, "LEFT", "RIGHT", 0).Result()
	if err != nil {
		return "", err
	}
	return jobId, nil
}

func AddJobPayloadHashToSet(ctx context.Context, payloadHash uint64) (bool, error) {
	result, err := client.SAdd(ctx, jobPayloadHashSet, payloadHash).Result()
	if err != nil {
		return false, err
	}
	if result == 0 {
		return false, nil
	}
	return true, nil
}

func SetJobIdToPayload(ctx context.Context, jobId string, payload []byte) error {
	_, err := client.Set(ctx, jobId, payload, 0).Result()
	if err != nil {
		return err
	}
	return nil
}

func PushJobIdToJobsQueue(ctx context.Context, jobId string) error {
	_, err := client.RPush(ctx, jobsQueue, jobId).Result()
	if err != nil {
		return err
	}
	return nil
}

func GetPayloadFromJobId(ctx context.Context, jobId string) ([]byte, error) {
	data, err := client.Get(ctx, jobId).Bytes()
	if err != nil {
		return nil, err
	}
	return data, nil
}
