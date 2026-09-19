package worker

import (
	"context"
	"fmt"
	"sync"

	"core-proxy/pkg/common/observability"
	"core-proxy/pkg/config"
	"core-proxy/pkg/control/health"
)

type Manager struct {
	mu        sync.RWMutex
	workers   map[string]*SSHWorker
	healthMgr *health.Manager
}

func NewManager(healthMgr *health.Manager) *Manager {
	return &Manager{
		workers:   make(map[string]*SSHWorker),
		healthMgr: healthMgr,
	}
}

func (m *Manager) SyncWithConfig(ctx context.Context, workerCfgs map[string]config.WorkerConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for id, worker := range m.workers {
		if _, exists := workerCfgs[id]; !exists {
			observability.Info("Removing old worker", "worker_id", id)
			worker.Stop()
			delete(m.workers, id)
		}
	}

	for id, wCfg := range workerCfgs {
		if _, exists := m.workers[id]; !exists {
			observability.Info("Spawning new worker instance", "worker_id", id)
			w := NewSSHWorker(wCfg, m.healthMgr)
			m.workers[id] = w
			w.Start(ctx)
		}
	}
}

func (m *Manager) GetWorker(id string) (*SSHWorker, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	w, exists := m.workers[id]
	if !exists {
		return nil, fmt.Errorf("worker not found: %s", id)
	}
	return w, nil
}

func (m *Manager) StopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, w := range m.workers {
		observability.Info("Stopping worker during shutdown", "worker_id", id)
		w.Stop()
	}
	m.workers = make(map[string]*SSHWorker)
}

