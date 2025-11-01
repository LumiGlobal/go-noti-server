package main

import (
	"go-noti-server/config"
	"go-noti-server/internal/nr"
	"go-noti-server/internal/server"
)

func main() {
	config.LoadEnv()
	nr.Init()
	server.RunGrpcServer()
}
