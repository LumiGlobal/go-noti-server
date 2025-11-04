package main

import (
	"go-noti-server/config"
	"go-noti-server/internal/datastore"
	"go-noti-server/internal/dispatcher"
	"go-noti-server/internal/fcm"
	"go-noti-server/internal/server"
	"go-noti-server/internal/telemetry"
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

	go dispatcher.Run(slotsChan, jobsChan)
	server.RunGrpcServer()
}
