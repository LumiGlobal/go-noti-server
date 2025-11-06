package main

import (
	"context"
	"go-noti-server/internal/datastore"
	"go-noti-server/internal/dispatcher"
	"go-noti-server/internal/notification"
	"go-noti-server/internal/server"
)

const numWorkers = 15

func main() {
	datastore.RequeueUnfinishedJobs(context.Background())
	slotsChan := make(chan struct{}, numWorkers)
	jobsChan := make(chan string, numWorkers)
	for i := 0; i < numWorkers; i++ {
		slotsChan <- struct{}{}
	}
	for i := 0; i < numWorkers; i++ {
		go notification.Worker(i, jobsChan, slotsChan)
	}

	go dispatcher.Run(slotsChan, jobsChan)
	server.RunGrpcServer()
}
