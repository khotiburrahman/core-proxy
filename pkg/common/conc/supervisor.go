package conc

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"

	"core-proxy/pkg/common/observability"
)

type Supervisor struct {
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

func NewSupervisor(parentCtx context.Context) *Supervisor {
	ctx, cancel := context.WithCancel(parentCtx)
	return &Supervisor{
		ctx:    ctx,
		cancel: cancel,
	}
}

func (s *Supervisor) Context() context.Context {
	return s.ctx
}

func (s *Supervisor) Go(name string, fn func(ctx context.Context)) {
	s.wg.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				stack := string(debug.Stack())
				observability.Error("Goroutine panic recovered",
					"routine_name", name,
					"panic_reason", fmt.Sprintf("%v", r),
					"stack_trace", stack,
				)
			}
			s.wg.Done()
		}()

		fn(s.ctx)
	}()
}

func (s *Supervisor) Stop() {
	s.cancel()
}

func (s *Supervisor) Wait() {
	s.wg.Wait()
}

func (s *Supervisor) CancelAndWait() {
	s.cancel()
	s.wg.Wait()
}

