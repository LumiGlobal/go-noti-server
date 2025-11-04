package fcm

import (
	"context"
	"fmt"
	"go-noti-server/internal/datastore"
	"go-noti-server/internal/telemetry"
	pb "go-noti-server/protos/notifications"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
)

func Worker(id int, jobsChan <-chan string, slotsChan chan<- struct{}) {
	telemetry.Log(zerolog.InfoLevel, fmt.Sprintf("[Worker %v] spawned", id))
	for jobId := range jobsChan {
		log(zerolog.InfoLevel, id, jobId, "received jobId")

		data, err := datastore.GetPayloadFromJobId(context.Background(), jobId)
		if err != nil {
			log(zerolog.ErrorLevel, id, jobId, fmt.Sprintf("ERROR RETRIEVING PAYLOAD: %v", err))
			return
		}
		log(zerolog.DebugLevel, id, jobId, "retrieve payload")

		var notification pb.NotificationPackage
		err = proto.Unmarshal(data, &notification)
		if err != nil {
			log(zerolog.ErrorLevel, id, jobId, fmt.Sprintf("ERROR UNMARSHALLING NOTIFICATION:: %v", err))
			return
		}
		log(zerolog.DebugLevel, id, jobId, "unmarshalled payload data")

		slotsChan <- struct{}{}
	}
}

func log(level zerolog.Level, id int, jobId string, msg string) {
	telemetry.Log(level, fmt.Sprintf("[Worker %v] [%v] %v", id, jobId, msg))
}
