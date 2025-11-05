package fcm

import (
	"context"
	"fmt"
	"go-noti-server/internal/datastore"
	"go-noti-server/internal/telemetry"
	pb "go-noti-server/protos/notifications"
	"net/http"
	"time"

	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
)

func Worker(id int, jobsChan <-chan string, slotsChan chan<- struct{}) {
	telemetry.Log(zerolog.InfoLevel, fmt.Sprintf("[Worker %v] spawned", id))
	for jobId := range jobsChan {
		txn := telemetry.App.StartTransaction(fmt.Sprintf("Worker %v", id))

		traceHeaders, _ := telemetry.GetTraceHeaders(jobId)
		if traceHeaders == nil {
			traceHeaders = http.Header{}
		}
		txn.AcceptDistributedTraceHeaders(newrelic.TransportOther, traceHeaders)

		ctx := newrelic.NewContext(context.Background(), txn)
		logger := telemetry.NewLogger(ctx)
		log(logger, zerolog.InfoLevel, id, jobId, "received jobId")

		data, err := datastore.GetPayloadFromJobId(ctx, jobId)
		if err != nil {
			txn.NoticeError(err)
			log(logger, zerolog.ErrorLevel, id, jobId, fmt.Sprintf("ERROR RETRIEVING PAYLOAD: %v", err))
			slotsChan <- struct{}{}
			continue
		}
		log(logger, zerolog.DebugLevel, id, jobId, "retrieve payload")

		seg := txn.StartSegment("UnmarshallingNotification")
		var notification pb.NotificationPackage
		err = proto.Unmarshal(data, &notification)
		if err != nil {
			txn.NoticeError(err)
			log(logger, zerolog.ErrorLevel, id, jobId, fmt.Sprintf("ERROR UNMARSHALLING NOTIFICATION: %v", err))
			slotsChan <- struct{}{}
			continue
		}
		seg.End()
		log(logger, zerolog.DebugLevel, id, jobId, "unmarshalled payload data")

		time.Sleep(20 * time.Second)
		log(logger, zerolog.InfoLevel, id, jobId, "payload sent to FCM")

		slotsChan <- struct{}{}

		txn.End()
	}
}

func log(logger zerolog.Logger, level zerolog.Level, id int, jobId string, msg string) {
	logger.WithLevel(level).
		Str("goroutine", fmt.Sprintf("worker %v", id)).
		Str("jobId", jobId).
		Msg(fmt.Sprintf("[Worker %v] [%v] %v", id, jobId, msg))
}
