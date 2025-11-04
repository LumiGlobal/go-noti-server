package main

import (
	"context"
	"fmt"
	"go-noti-server/config"
	"go-noti-server/internal/datastore"
	"go-noti-server/internal/fcm"
	"go-noti-server/internal/server"
	"go-noti-server/internal/telemetry"
	"time"

	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/rs/zerolog"
)

const numWorkers = 15

func main() {
	config.LoadEnv()
	datastore.Init()
	telemetry.Init()

	slotsChan := make(chan struct{}, numWorkers)
	jobsChan := make(chan string, numWorkers)
	for i := 0; i < numWorkers; i++ {
		slotsChan <- struct{}{}
	}
	for i := 0; i < numWorkers; i++ {
		go fcm.Worker(i, jobsChan, slotsChan)
	}

	go func(slotsChan chan struct{}, jobsChan chan<- string) {
		for {
			txn := telemetry.App.StartTransaction("QueueDispatcher")
			ctx := newrelic.NewContext(context.Background(), txn)

			t := time.Now()
			<-slotsChan
			telemetry.Log(zerolog.DebugLevel, fmt.Sprintf("[Dispatcher] Free slot after %v", time.Since(t)))

			t = time.Now()
			jobId, err := datastore.MoveJobToProcessing(ctx)
			if err != nil {
				msg := fmt.Sprintf("[Dispatcher] [%v] ERROR MOVING JOB ID FROM JOBS TO PROCESSING: %v", jobId, err)
				telemetry.Log(zerolog.ErrorLevel, msg)
				slotsChan <- struct{}{}
				continue
			}
			telemetry.Log(zerolog.DebugLevel, fmt.Sprintf("[Dispatcher] Received job after %v", time.Since(t)))
			telemetry.Log(zerolog.DebugLevel, fmt.Sprintf("[Dispatcher] [%v] BLMOVE from jobs to processing", jobId))

			jobsChan <- jobId
			telemetry.Log(zerolog.InfoLevel, fmt.Sprintf("[Dispatcher] [%v] pushed to jobsChan", jobId))

			txn.End()
		}
	}(slotsChan, jobsChan)
	server.RunGrpcServer()
}
