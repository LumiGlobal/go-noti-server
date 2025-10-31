package main

import (
	"go-noti-server/config"
	"go-noti-server/internal/apm"
	"time"
)

func main() {
	config.LoadEnv()
	apm.Init()
	apm.Log.Info().Msg("TESTTTTTTTTTTTTTTTTTTTTTTT")
	time.Sleep(1 * time.Minute)
	apm.App.Shutdown(5 * time.Second)
}
