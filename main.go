package main

import (
	"context"
	"fmt"
	"go-noti-server/config"
	"go-noti-server/internal/datastore"
	"go-noti-server/internal/server"
	"go-noti-server/internal/telemetry"

	"github.com/rs/zerolog"
)

func main() {
	config.LoadEnv()
	datastore.Init()
	telemetry.Init()

	numWorkers := 15
	slotsChan := make(chan struct{}, numWorkers)
	jobsChan := make(chan string, numWorkers)
	for i := 0; i < numWorkers; i++ {
		slotsChan <- struct{}{}
	}

	go func(slotsChan <-chan struct{}, jobsChan chan<- string) {
		ctx := context.Background()
		for {
			<-slotsChan
			jobId, err := datastore.MoveJobToProcessing(ctx)
			if err != nil {
				telemetry.Log(zerolog.ErrorLevel, fmt.Sprintf("ERROR MOVING JOB ID FROM JOBS TO PROCESSING: %v", err))
			}
			telemetry.Log(zerolog.DebugLevel, fmt.Sprintf("[%v] BLMOVE from jobs to processing", jobId))
			jobsChan <- jobId
			telemetry.Log(zerolog.DebugLevel, fmt.Sprintf("[%v] pushed to jobsChan", jobId))
		}
	}(slotsChan, jobsChan)
	server.RunGrpcServer()
}
