package nr

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/newrelic/go-agent/v3/integrations/logcontext-v2/zerologWriter"
	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/rs/zerolog"
)

var App *newrelic.Application

func Init() {
	var err error

	App, err = newrelic.NewApplication(
		newrelic.ConfigAppName(os.Getenv("NEW_RELIC_APP_NAME")),
		newrelic.ConfigLicense(os.Getenv("NEW_RELIC_LICENSE_KEY")),
	)

	if err != nil {
		log.Fatalln("Fail to crate New Relic Application")
	}
}

func Logger() zerolog.Logger {
	return zerolog.New(writer()).
		Level(zerolog.InfoLevel).
		With().
		Timestamp().
		Logger()
}

func writer() zerologWriter.ZerologWriter {
	consoleWriter := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339,
	}
	return zerologWriter.New(consoleWriter, App)
}

func TxnLogger(txn *newrelic.Transaction) zerolog.Logger {
	w := writer()
	txnWriter := w.WithTransaction(txn)
	return Logger().Output(txnWriter)
}

func TxnCtxLogger(ctx context.Context) zerolog.Logger {
	w := writer()
	txnWriter := w.WithContext(ctx)
	return Logger().Output(txnWriter)
}
