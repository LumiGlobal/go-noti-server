package telemetry

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/newrelic/go-agent/v3/integrations/logcontext-v2/zerologWriter"
	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/rs/zerolog"
)

var (
	App             *newrelic.Application
	logger          zerolog.Logger
	traceHeadersMap = &safeMap{m: make(map[string]http.Header)}
)

type safeMap struct {
	mu sync.Mutex
	m  map[string]http.Header
}

func Init() {
	var err error

	App, err = newrelic.NewApplication(
		newrelic.ConfigAppName(os.Getenv("NEW_RELIC_APP_NAME")),
		newrelic.ConfigLicense(os.Getenv("NEW_RELIC_LICENSE_KEY")),
		newrelic.ConfigDistributedTracerEnabled(true),
	)

	if err != nil {
		log.Fatalln("Fail to create New Relic Application")
	}

	err = App.WaitForConnection(10 * time.Second)
	if err != nil {
		log.Fatalln("Fail to connect to New Relic")
	}

	logger = newLogger()
}

func NewLogger(ctx context.Context) zerolog.Logger {
	w := newWriter()
	txnWriter := w.WithContext(ctx)
	return newLogger().Output(txnWriter)
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

func AddTraceHeaders(key string, traceHeaders http.Header) {
	traceHeadersMap.mu.Lock()
	traceHeadersMap.m[key] = traceHeaders
	traceHeadersMap.mu.Unlock()
}

func GetTraceHeaders(key string) (http.Header, bool) {
	traceHeadersMap.mu.Lock()
	defer traceHeadersMap.mu.Unlock()
	header, ok := traceHeadersMap.m[key]
	return header, ok
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
