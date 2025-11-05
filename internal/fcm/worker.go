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
	log(logger, zerolog.InfoLevel, workerId, jobId, "Starting job")

	data, err := datastore.GetPayloadFromJobId(ctx, jobId)
	if err != nil {
		txn.NoticeError(err)
		log(logger, zerolog.ErrorLevel, workerId, jobId, fmt.Sprintf("ERROR RETRIEVING PAYLOAD: %v", err))
		return
	}

	seg := txn.StartSegment("UnmarshallingNotification")
	var notification pb.NotificationPackage
	err = proto.Unmarshal(data, &notification)
	if err != nil {
		txn.NoticeError(err)
		log(logger, zerolog.ErrorLevel, workerId, jobId, fmt.Sprintf("ERROR UNMARSHALLING NOTIFICATION: %v", err))
		return
	}
	seg.End()

	//fcmMsg := getFcmMessage(&notification)
	seg = txn.StartSegment("SendingFCMMessage")
	time.Sleep(60 * time.Second)
	//t := time.Now()
	//resp, err := client.SendEachForMulticastDryRun(ctx, fcmMsg)
	//if err != nil {
	//	txn.NoticeError(err)
	//	log(logger, zerolog.ErrorLevel, workerId, jobId, fmt.Sprintf("ERROR SENDING MESSAGE TO FCM: %v", err))
	//	return
	//}
	//if resp == nil {
	//	err := fmt.Errorf("BatchResponse is nil")
	//	txn.NoticeError(err)
	//	log(logger, zerolog.ErrorLevel, workerId, jobId, fmt.Sprintf("ERROR RESPONSE FROM FCM: %v", err))
	//	return
	//}
	seg.End()
	//msg := fmt.Sprintf("FCM message sent | FCM Time: %v, SuccessCount: %v, FailureCount: %v", time.Since(t), resp.SuccessCount, resp.FailureCount)
	//log(logger, zerolog.InfoLevel, workerId, jobId, msg)

	err = datastore.RemoveJobIdFromProcessing(ctx, jobId)
	if err != nil {
		txn.NoticeError(err)
		log(logger, zerolog.ErrorLevel, workerId, jobId, fmt.Sprintf("ERROR REMOVING JOB ID FROM PROCESSING: %v", err))
		return
	}

	err = datastore.RemovePayload(ctx, jobId)
	if err != nil {
		txn.NoticeError(err)
		log(logger, zerolog.ErrorLevel, workerId, jobId, fmt.Sprintf("ERROR REMOVING PAYLOAD: %v", err))
		return
	}
	log(logger, zerolog.InfoLevel, workerId, jobId, "Finished job")
}

func getFcmMessage(notification *pb.NotificationPackage) *messaging.MulticastMessage {
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

func freeSlot(slotsChan chan<- struct{}) {
	slotsChan <- struct{}{}
}

func log(logger zerolog.Logger, level zerolog.Level, id int, jobId string, msg string) {
	logger.WithLevel(level).
		Str("goroutine", fmt.Sprintf("worker %v", id)).
		Str("jobId", jobId).
		Msg(fmt.Sprintf("[Worker %v] [%v] %v", id, jobId, msg))
}
