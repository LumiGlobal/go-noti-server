package fcm

import (
	"context"
	"fmt"
	"go-noti-server/config"
	"go-noti-server/internal/datastore"
	"go-noti-server/internal/telemetry"
	pb "go-noti-server/protos/notifications"
	"log"
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

func init() {
	config.LoadEnv()
	var err error
	auth := os.Getenv("AUTH_FILE")
	opts := option.WithCredentialsFile(auth)
	ctx := context.Background()
	app, err = firebase.NewApp(ctx, nil, opts)
	if err != nil {
		log.Fatalf(fmt.Sprintf("error creating new firebase app: %v", err))
	}
	client, err = app.Messaging(ctx)
	if err != nil {
		log.Fatalf(fmt.Sprintf("error creating new firebase client: %v", err))
	}
}

func Worker(id int, jobsChan <-chan string, slotsChan chan<- struct{}) {
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

	logger := newFcmLogger(ctx, workerId, jobId)
	logger.log(zerolog.InfoLevel, "starting job")

	data, err := datastore.GetPayloadFromJobId(ctx, jobId)
	if err != nil {
		txn.NoticeError(err)
		logger.log(zerolog.ErrorLevel, fmt.Sprintf("ERROR RETRIEVING PAYLOAD: %v", err))
		return
	}

	seg := txn.StartSegment("UnmarshallingNotification")
	var notification pb.NotificationPackage
	err = proto.Unmarshal(data, &notification)
	if err != nil {
		txn.NoticeError(err)
		logger.log(zerolog.ErrorLevel, fmt.Sprintf("ERROR UNMARSHALLING NOTIFICATION: %v", err))
		return
	}
	seg.End()

	seg = txn.StartSegment("SendFCMMessage")
	multicastMsg := newMulticastMessage(&notification)
	t := time.Now()
	resp, err := client.SendEachForMulticast(ctx, multicastMsg)
	if err != nil {
		txn.NoticeError(err)
		logger.log(zerolog.ErrorLevel, fmt.Sprintf("ERROR SENDING MESSAGE TO FCM: %v", err))
		return
	}
	if resp == nil {
		err := fmt.Errorf("BatchResponse is nil")
		txn.NoticeError(err)
		logger.log(zerolog.ErrorLevel, fmt.Sprintf("ERROR RESPONSE FROM FCM: %v", err))
		return
	}
	seg.End()
	msg := fmt.Sprintf("FCM message sent | FCM Time: %v, TotalTokens: %v, SuccessCount: %v, FailureCount: %v", time.Since(t), len(multicastMsg.Tokens), resp.SuccessCount, resp.FailureCount)
	logger.log(zerolog.InfoLevel, msg)

	err = cleanupJob(ctx, jobId)
	if err != nil {
		txn.NoticeError(err)
		logger.log(zerolog.ErrorLevel, fmt.Sprintf("ERROR DURING CLEANUP JOB: %v", err))
		return
	}

	logger.log(zerolog.InfoLevel, "Finished job")
}

func freeSlot(slotsChan chan<- struct{}) {
	slotsChan <- struct{}{}
}

type fcmLogger struct {
	logger   zerolog.Logger
	workerId int
	jobId    string
}

func (fl *fcmLogger) log(level zerolog.Level, msg string) {
	fl.logger.WithLevel(level).
		Str("goroutine", fmt.Sprintf("worker %v", fl.workerId)).
		Str("jobId", fl.jobId).
		Msg(fmt.Sprintf("[Worker %v] [%v] %v", fl.workerId, fl.jobId, msg))
}

func newFcmLogger(ctx context.Context, workerId int, jobId string) fcmLogger {
	return fcmLogger{
		logger:   telemetry.NewLogger(ctx),
		workerId: workerId,
		jobId:    jobId,
	}
}

func newMulticastMessage(notification *pb.NotificationPackage) *messaging.MulticastMessage {
	var channelId string

	value, ok := notification.Data["channelId"]
	if ok {
		channelId = value
	}
	return &messaging.MulticastMessage{
		Android: &messaging.AndroidConfig{
			Priority: "high",
			Notification: &messaging.AndroidNotification{
				Title:     notification.Title,
				Body:      notification.Body,
				ImageURL:  notification.Image,
				ChannelID: channelId,
				Proxy:     messaging.ProxyDeny,
			},
			Data: notification.Data,
		},
		APNS: &messaging.APNSConfig{
			Headers: map[string]string{
				"apns-priority": "10",
			},
			Payload: &messaging.APNSPayload{
				Aps: &messaging.Aps{
					Alert: &messaging.ApsAlert{
						Title:       notification.Title,
						Body:        notification.Body,
						LaunchImage: notification.Image,
					},
					Sound: "default",
				},
				CustomData: map[string]interface{}{
					"image-url": notification.Image,
					"data":      notification.Data,
				},
			},
		},
		Notification: &messaging.Notification{
			Title:    notification.Title,
			Body:     notification.Body,
			ImageURL: notification.Image,
		},
		FCMOptions: &messaging.FCMOptions{
			AnalyticsLabel: notification.AnalyticsLabel,
		},
		Tokens: notification.DeviceTokens,
		Data:   notification.Data,
	}
}

func cleanupJob(ctx context.Context, jobId string) error {
	err := datastore.RemoveJobIdFromProcessing(ctx, jobId)
	if err != nil {
		return fmt.Errorf("error removing job id from processing: %v", err)
	}
	err = datastore.RemovePayload(ctx, jobId)
	if err != nil {
		return fmt.Errorf("error removing payload: %v", err)
	}
	telemetry.DeleteTraceHeaders(jobId)
	return nil
}
