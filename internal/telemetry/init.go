package telemetry

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

var (
	App    *newrelic.Application
	logger zerolog.Logger
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
	logger = newLogger()
}

func NewLogger(ctx context.Context) zerolog.Logger {
	w := newWriter()
	txnWriter := w.WithContext(ctx)
	return newLogger().Output(txnWriter)
}

func MsgWithTraceID(ctx context.Context, msg string) string {
	traceID := newrelic.FromContext(ctx).GetTraceMetadata().TraceID
	return fmt.Sprintf("[%v] %v", traceID, msg)
}

func LogWithContext(level zerolog.Level, msg string, ctx context.Context) {
	txnlogger := NewLogger(ctx)
	txn := newrelic.FromContext(ctx)
	metadata := txn.GetTraceMetadata()
	txnlogger.WithLevel(level).Msg(fmt.Sprintf("[%v] %v", metadata.TraceID, msg))
}

func Log(level zerolog.Level, msg string) {
	logger.WithLevel(level).Msg(msg)
}

func newLogger() zerolog.Logger {
	return zerolog.New(newWriter()).
		Level(zerolog.InfoLevel).
		With().
		Timestamp().
		Logger()
}

func newWriter() zerologWriter.ZerologWriter {
	consoleWriter := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339,
	}
	return zerologWriter.New(consoleWriter, App)
}
