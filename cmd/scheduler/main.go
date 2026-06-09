// Command scheduler promotes due scheduled notifications into the work queue.
package main

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/ibrahim-bg/notifier/internal/app"
	"github.com/ibrahim-bg/notifier/internal/scheduler"
)

func main() {
	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	c, err := app.New(rootCtx, "scheduler")
	if err != nil {
		panic(err)
	}
	defer c.Close(context.Background())

	c.Logger.Info("scheduler started", "interval", c.Cfg.SchedulerPollInterval.String())
	scheduler.Run(rootCtx, scheduler.QueuePromoter{Src: c.Scheduled, Dst: c.Queue}, c.Cfg.SchedulerPollInterval)
	c.Logger.Info("scheduler stopped")
}
