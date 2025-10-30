package main

import (
	"go-noti-server/config"
	"go-noti-server/internal/log"
	"go-noti-server/internal/server"
)

func main() {
	config.LoadEnv()
	log.SetupLoggers()
	server.RunGrpcServer()
}
