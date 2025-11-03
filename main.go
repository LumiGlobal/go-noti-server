package main

import (
	"go-noti-server/config"
	"go-noti-server/internal/datastore"
	"go-noti-server/internal/server"
	"go-noti-server/internal/telemetry"
)

func main() {
	config.LoadEnv()
	datastore.Init()
	telemetry.Init()
	server.RunGrpcServer()
}
