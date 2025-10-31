package apm

import (
	"log"
	"os"
	"time"

	"github.com/newrelic/go-agent/v3/integrations/logcontext-v2/zerologWriter"
	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/rs/zerolog"
)

var (
	App *newrelic.Application
	Log zerolog.Logger
)

func Init() {
	var err error

	App, err = newrelic.NewApplication(
		newrelic.ConfigAppName(os.Getenv("NEW_RELIC_APP_NAME")),
		newrelic.ConfigLicense(os.Getenv("NEW_RELIC_LICENSE_KEY")),
	)

	if err != nil {
		log.Fatalln("Fail to crate New Relic Application")
	}

	consoleWriter := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339,
	}

	writer := zerologWriter.New(consoleWriter, App)
	Log = zerolog.New(writer).
		Level(zerolog.InfoLevel).
		With().
		Timestamp().
		Logger()
}
