package main

import (
	"go-noti-server/config"
	"go-noti-server/internal/log"
)

func main() {
	config.LoadEnv()
	log.SetupLogger()
	log.Logger.
		Info().
		Str("job_id", "123456699").
		Msg("LOGGING")
}
