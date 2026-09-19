package outbound

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"core-proxy/pkg/common/observability"
	"core-proxy/pkg/config"
	"core-proxy/pkg/control/outbound/balancer"
	"core-proxy/pkg/control/worker"
)

type OutboundType string

const (
	TypeDirect OutboundType = "direct"
	TypeWorker OutboundType = "worker"
	TypePool   OutboundType = "pool"
)

type OutboundTarget struct {
	Name      string
	Type      OutboundType
	WorkerID  string
	WorkerIDs []string
	Balancer  balancer.LoadBalancer
}

type Manager struct {
	mu        sync.RWMutex
	outbounds map[string]*OutboundTarget
	workerMgr *worker.Manager
}

func NewManager(workerMgr *worker.Manager) *Manager {
	return &Manager{
		outbounds: make(map[string]*OutboundTarget),
		workerMgr: workerMgr,
	}
}

func (m *Manager) SyncWithConfig(cfgs map[string]config.OutboundConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()

	newOutbounds := make(map[string]*OutboundTarget)

	for name, cfg := range cfgs {
		target := &OutboundTarget{
			Name: name,
			Type: OutboundType(cfg.Type),
		}

		switch target.Type {
		case TypeWorker:
			if len(cfg.WorkerIDs) > 0 {
				target.WorkerID = cfg.WorkerIDs[0]
			}
		case TypePool:
			target.WorkerIDs = cfg.WorkerIDs
			switch balancer.Strategy(cfg.Strategy) {
			case balancer.StrategyLeastConn:
				target.Balancer = balancer.NewLeastConn()
			case balancer.StrategyAdaptive:
				target.Balancer = balancer.NewAdaptiveHysteresis(5*time.Second, 0.2)
			default:
				target.Balancer = balancer.NewRoundRobin()
			}
		}

		newOutbounds[name] = target
	}

	m.outbounds = newOutbounds
	observability.Info("Outbound Manager synced with config", "count", len(m.outbounds))
}

func (m *Manager) DialContext(ctx context.Context, outboundName string, network, address string) (net.Conn, error) {
	m.mu.RLock()
	target, exists := m.outbounds[outboundName]
	m.mu.RUnlock()

	if !exists {
		observability.Warn("Outbound target not found, falling back to DIRECT", "outbound", outboundName)
		var dialer net.Dialer
		return dialer.DialContext(ctx, network, address)
	}

	switch target.Type {
	case TypeDirect:
		var dialer net.Dialer
		return dialer.DialContext(ctx, network, address)

	case TypeWorker:
		w, err := m.workerMgr.GetWorker(target.WorkerID)
		if err != nil {
			return nil, err
		}
		return m.dialViaWorker(ctx, w, network, address)

	case TypePool:
		var poolWorkers []*worker.SSHWorker
		for _, id := range target.WorkerIDs {
			if w, err := m.workerMgr.GetWorker(id); err == nil {
				poolWorkers = append(poolWorkers, w)
			}
		}

		selectedWorker, err := target.Balancer.Select(ctx, poolWorkers)
		if err != nil {
			return nil, fmt.Errorf("outbound pool %s selection failed: %w", target.Name, err)
		}

		return m.dialViaWorker(ctx, selectedWorker, network, address)

	default:
		return nil, fmt.Errorf("unsupported outbound type: %s", target.Type)
	}
}

func (m *Manager) dialViaWorker(ctx context.Context, w *worker.SSHWorker, network, address string) (net.Conn, error) {
	w.IncrConn()
	conn, err := w.DialChannel(ctx, network, address)
	if err != nil {
		w.DecrConn()
		return nil, err
	}

	return &trackedConn{
		Conn:   conn,
		worker: w,
	}, nil
}

type trackedConn struct {
	net.Conn
	worker *worker.SSHWorker
	once   sync.Once
}

func (tc *trackedConn) Close() error {
	var err error
	tc.once.Do(func() {
		tc.worker.DecrConn()
		err = tc.Conn.Close()
	})
	return err
}

