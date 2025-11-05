package fcm

import (
	"context"
	"fmt"
	"go-noti-server/internal/datastore"
	"go-noti-server/internal/telemetry"
	pb "go-noti-server/protos/notifications"
	"net/http"
	"os"
	"time"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/rs/zerolog"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/proto"
)

var (
	app    *firebase.App
	client *messaging.Client
)

func Init() {
	var err error
	auth := os.Getenv("AUTH_FILE")
	opts := option.WithCredentialsFile(auth)
	ctx := context.Background()
	app, err = firebase.NewApp(ctx, nil, opts)
	if err != nil {
		telemetry.Log(zerolog.FatalLevel, fmt.Sprintf("error creating new firebase app: %v", err))
	}
	client, err = app.Messaging(ctx)
	if err != nil {
		telemetry.Log(zerolog.FatalLevel, fmt.Sprintf("error creating new firebase client: %v", err))
	}
}

func Worker(id int, jobsChan <-chan string, slotsChan chan<- struct{}) {
	telemetry.Log(zerolog.InfoLevel, fmt.Sprintf("[Worker %v] spawned", id))
	for jobId := range jobsChan {
		processJob(id, jobId, slotsChan)
	}
}

func processJob(workerId int, jobId string, slotsChan chan<- struct{}) {
	defer freeSlot(slotsChan)

	txn := telemetry.App.StartTransaction(fmt.Sprintf("Worker %v", workerId))
	defer txn.End()

	traceHeaders, _ := telemetry.GetTraceHeaders(jobId)
	if traceHeaders == nil {
		traceHeaders = http.Header{}
	}
	txn.AcceptDistributedTraceHeaders(newrelic.TransportOther, traceHeaders)
	ctx := newrelic.NewContext(context.Background(), txn)

	logger := telemetry.NewLogger(ctx)
	log(logger, zerolog.InfoLevel, workerId, jobId, "received jobId")

	data, err := datastore.GetPayloadFromJobId(ctx, jobId)
	if err != nil {
		txn.NoticeError(err)
		log(logger, zerolog.ErrorLevel, workerId, jobId, fmt.Sprintf("ERROR RETRIEVING PAYLOAD: %v", err))
		return
	}
	log(logger, zerolog.DebugLevel, workerId, jobId, "retrieve payload")

	seg := txn.StartSegment("UnmarshallingNotification")
	var notification pb.NotificationPackage
	err = proto.Unmarshal(data, &notification)
	if err != nil {
		txn.NoticeError(err)
		log(logger, zerolog.ErrorLevel, workerId, jobId, fmt.Sprintf("ERROR UNMARSHALLING NOTIFICATION: %v", err))
		return
	}
	seg.End()
	log(logger, zerolog.DebugLevel, workerId, jobId, "unmarshalled payload data")

	time.Sleep(30 * time.Second)
}

func freeSlot(slotsChan chan<- struct{}) {
	slotsChan <- struct{}{}
}

func log(logger zerolog.Logger, level zerolog.Level, id int, jobId string, msg string) {
	logger.WithLevel(level).
		Str("goroutine", fmt.Sprintf("worker %v", id)).
		Str("jobId", jobId).
		Msg(fmt.Sprintf("[Worker %v] [%v] %v", id, jobId, msg))
}
