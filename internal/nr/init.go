package nr

import (
	"context"
	"fmt"
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

func MsgFormatter(ctx context.Context, msg string) string {
	traceID := newrelic.FromContext(ctx).GetTraceMetadata().TraceID
	return fmt.Sprintf("[%v] %v", traceID, msg)
}

func LogWithContext(level zerolog.Level, msg string, ctx context.Context) {
	txnlogger := ContextLogger(ctx)
	txn := newrelic.FromContext(ctx)
	metadata := txn.GetTraceMetadata()
	txnlogger.WithLevel(level).Msg(fmt.Sprintf("[%v] %v", metadata.TraceID, msg))
}

func Log(level zerolog.Level, msg string) {
	logger := getLogger()
	logger.WithLevel(level).Msg(msg)
}

func getLogger() zerolog.Logger {
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

func txnLogger(txn *newrelic.Transaction) zerolog.Logger {
	w := writer()
	txnWriter := w.WithTransaction(txn)
	return getLogger().Output(txnWriter)
}

func ContextLogger(ctx context.Context) zerolog.Logger {
	w := writer()
	txnWriter := w.WithContext(ctx)
	return getLogger().Output(txnWriter)
}
