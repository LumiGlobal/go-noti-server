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
		data, err := datastore.GetPayloadFromJobId(context.Background(), jobId)
		if err != nil {
			telemetry.Log(zerolog.ErrorLevel, fmt.Sprintf("ERROR RETRIEVING PAYLOAD FOR JOB %v: %v", jobId, err))
			return
		}

		var notification pb.NotificationPackage
		err = proto.Unmarshal(data, &notification)
		if err != nil {
			telemetry.Log(zerolog.ErrorLevel, fmt.Sprintf("ERROR UNMARSHALLING NOTIFICATION %v: %v", jobId, err))
			return
		}

		slotsChan <- struct{}{}
	}
}
