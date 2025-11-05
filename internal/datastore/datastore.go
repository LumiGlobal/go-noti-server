package datastore

import (
	"context"
	"errors"
	"fmt"
	"go-noti-server/internal/telemetry"
	"log"
	"os"
	"time"

	"github.com/newrelic/go-agent/v3/integrations/nrredis-v9"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
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

	err := client.Ping(context.Background()).Err()
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
	err := client.Set(ctx, jobId, payload, 0).Err()
	if err != nil {
		return err
	}
	return nil
}

func PushJobIdToJobsQueue(ctx context.Context, jobId string) error {
	err := client.RPush(ctx, jobsQueue, jobId).Err()
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

func RemoveJobIdFromProcessing(ctx context.Context, jobId string) error {
	err := client.LRem(ctx, processingQueue, 1, jobId).Err()
	if err != nil {
		return err
	}
	return nil
}

func RemovePayload(ctx context.Context, jobId string) error {
	err := client.Del(ctx, jobId).Err()
	if err != nil {
		return err
	}
	return nil
}

func RequeueUnfinishedJobs(ctx context.Context) {
	i := 0
	for {
		err := client.LMove(ctx, processingQueue, jobsQueue, "RIGHT", "LEFT").Err()
		if errors.Is(err, redis.Nil) {
			if i == 0 {
				telemetry.Log(zerolog.InfoLevel, "No unfinished jobs")
			} else {
				telemetry.Log(zerolog.InfoLevel, fmt.Sprintf("Moved %v jobs from jobs queue to processing queue", i))
			}
			break
		}
		if err != nil {
			telemetry.Log(zerolog.FatalLevel, fmt.Sprintf("error requeuing unfinished jobs: %v", err))
			break
		}
		i++
	}
}
