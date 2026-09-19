package shutdown

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"core-proxy/pkg/common/observability"
)

type CleanupFunc func(ctx context.Context)

type Coordinator struct {
	timeout time.Duration
	tasks   []CleanupFunc
}

func NewCoordinator(timeout time.Duration) *Coordinator {
	return &Coordinator{timeout: timeout}
}

func (c *Coordinator) Register(fn CleanupFunc) {
	c.tasks = append(c.tasks, fn)
}

func (c *Coordinator) WaitAndShutdown() {
	stopSig := make(chan os.Signal, 1)
	signal.Notify(stopSig, os.Interrupt, syscall.SIGTERM)

	<-stopSig
	observability.Info("Shutdown signal received, starting graceful termination...")

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	for i := len(c.tasks) - 1; i >= 0; i-- {
		c.tasks[i](ctx)
	}

	observability.Info("All components stopped. Bye!")
}

