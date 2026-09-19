package balancer

import (
	"context"

	"core-proxy/pkg/control/worker"
)

type Strategy string

const (
	StrategyRoundRobin Strategy = "round_robin"
	StrategyLeastConn  Strategy = "least_conn"
	StrategyLatency    Strategy = "latency"
	StrategyAdaptive   Strategy = "adaptive"
)

type LoadBalancer interface {
	Select(ctx context.Context, workers []*worker.SSHWorker) (*worker.SSHWorker, error)
}

