package main

import (
	"go-noti-server/config"
	"go-noti-server/internal/rd"
	"go-noti-server/internal/server"
	"go-noti-server/internal/telemetry"
)

func main() {
	config.LoadEnv()
	rd.Init()
	telemetry.Init()
	server.RunGrpcServer()
}
