package dispatcher

import (
	"context"
	"fmt"
	"go-noti-server/internal/datastore"
	"go-noti-server/internal/telemetry"
	"time"

	"github.com/newrelic/go-agent/v3/newrelic"
)

func Run(slotsChan chan struct{}, jobsChan chan<- string) {
	for {
		txn := telemetry.App.StartTransaction("QueueDispatcher")
		ctx := newrelic.NewContext(context.Background(), txn)
		logger := telemetry.NewLogger(ctx)

		t := time.Now()
		<-slotsChan
		logger.Debug().Msg(fmt.Sprintf("[Dispatcher] Free slot available after %v", time.Since(t)))

		t = time.Now()
		jobId, err := datastore.MoveJobToProcessing(ctx)
		if err != nil {
			logger.Error().Msg(fmt.Sprintf("[Dispatcher] ERROR MOVING JOB ID FROM JOBS TO PROCESSING: %v", err))
			slotsChan <- struct{}{}
			continue
		}
		logger.Debug().
			Str("jobId", jobId).
			Msg(fmt.Sprintf("[Dispatcher] [%v] Received job after waiting %v", jobId, time.Since(t)))

		logger.Debug().
			Str("jobId", jobId).
			Msg(fmt.Sprintf("[Dispatcher] [%v] BLMOVE from jobs to processing", jobId))

		jobsChan <- jobId

		logger.Info().
			Str("jobId", jobId).
			Msg(fmt.Sprintf("[Dispatcher] [%v] pushed to jobsChan", jobId))

		txn.End()
	}
}
