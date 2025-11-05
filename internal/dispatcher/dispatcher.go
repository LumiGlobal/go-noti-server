package dispatcher

import (
	"context"
	"fmt"
	"go-noti-server/internal/datastore"
	"go-noti-server/internal/telemetry"

	"github.com/newrelic/go-agent/v3/newrelic"
)

func Run(slotsChan chan struct{}, jobsChan chan<- string) {
	for {
		txn := telemetry.App.StartTransaction("QueueDispatcher")
		ctx := newrelic.NewContext(context.Background(), txn)
		logger := telemetry.NewLogger(ctx)

		<-slotsChan

		jobId, err := datastore.MoveJobToProcessing(ctx)
		if err != nil {
			txn.NoticeError(err)
			logger.Error().
				Str("goroutine", "dispatcher").
				Msg(fmt.Sprintf("[Dispatcher] ERROR MOVING JOB ID FROM JOBS TO PROCESSING: %v", err))
			slotsChan <- struct{}{}
			continue
		}

		jobsChan <- jobId

		logger.Info().
			Str("goroutine", "dispatcher").
			Str("jobId", jobId).
			Msg(fmt.Sprintf("[Dispatcher] [%v] pushed to jobsChan", jobId))

		txn.End()
	}
}
