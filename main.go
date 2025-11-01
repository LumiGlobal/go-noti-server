package main

import (
	"go-noti-server/config"
	"go-noti-server/internal/nr"
	"time"
)

func main() {
	config.LoadEnv()
	nr.Init()
	for {
		mainLogger := nr.Logger()
		txn := nr.App.StartTransaction("test test test")
		txnLogger := nr.TxnLogger(txn)
		txnLogger.Info().Str("test", "test").Msg("testing txn logger")
		txn.End()
		mainLogger.Info().Msg("done testing txn logger")
		time.Sleep(90 * time.Second)
	}
}
