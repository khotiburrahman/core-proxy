package balancer

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"core-proxy/pkg/control/worker"
)

type RoundRobinBalancer struct {
	index uint64
}

func NewRoundRobin() *RoundRobinBalancer {
	return &RoundRobinBalancer{}
}

func (rr *RoundRobinBalancer) Select(ctx context.Context, workers []*worker.SSHWorker) (*worker.SSHWorker, error) {
	var ready []*worker.SSHWorker
	for _, w := range workers {
		if w.State() == worker.StateRunning || w.State() == worker.StateReady {
			ready = append(ready, w)
		}
	}

	if len(ready) == 0 {
		return nil, fmt.Errorf("no healthy workers available in pool")
	}

	idx := atomic.AddUint64(&rr.index, 1)
	return ready[idx%uint64(len(ready))], nil
}

type LeastConnBalancer struct{}

func NewLeastConn() *LeastConnBalancer {
	return &LeastConnBalancer{}
}

func (lc *LeastConnBalancer) Select(ctx context.Context, workers []*worker.SSHWorker) (*worker.SSHWorker, error) {
	var selected *worker.SSHWorker
	minConn := int64(^uint64(0) >> 1)

	for _, w := range workers {
		if w.State() == worker.StateRunning || w.State() == worker.StateReady {
			active := w.ActiveConnections()
			if active < minConn {
				minConn = active
				selected = w
			}
		}
	}

	if selected == nil {
		return nil, fmt.Errorf("no healthy workers available in pool")
	}

	return selected, nil
}

type AdaptiveHysteresisBalancer struct {
	lastSelected   *worker.SSHWorker
	lastSelectTime time.Time
	cooldown       time.Duration
	epsilonRatio   float64
}

func NewAdaptiveHysteresis(cooldown time.Duration, epsilon float64) *AdaptiveHysteresisBalancer {
	return &AdaptiveHysteresisBalancer{
		cooldown:     cooldown,
		epsilonRatio: epsilon,
	}
}

func (ab *AdaptiveHysteresisBalancer) Select(ctx context.Context, workers []*worker.SSHWorker) (*worker.SSHWorker, error) {
	var best *worker.SSHWorker
	bestScore := float64(^uint64(0) >> 1)

	for _, w := range workers {
		if w.State() == worker.StateRunning || w.State() == worker.StateReady {
			score := float64(w.ActiveConnections()*10) + float64(w.Latency().Milliseconds())
			if score < bestScore {
				bestScore = score
				best = w
			}
		}
	}

	if best == nil {
		return nil, fmt.Errorf("no healthy workers available in pool")
	}

	if ab.lastSelected != nil && ab.lastSelected.State() == worker.StateRunning {
		if time.Since(ab.lastSelectTime) < ab.cooldown {
			currentScore := float64(ab.lastSelected.ActiveConnections()*10) + float64(ab.lastSelected.Latency().Milliseconds())
			if currentScore-bestScore < (currentScore * ab.epsilonRatio) {
				return ab.lastSelected, nil
			}
		}
	}

	ab.lastSelected = best
	ab.lastSelectTime = time.Now()
	return best, nil
}

