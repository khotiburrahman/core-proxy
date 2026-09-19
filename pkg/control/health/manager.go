package health

import (
	"sync"
	"time"
)

type HealthStatus string

const (
	StatusHealthy   HealthStatus = "HEALTHY"
	StatusDegraded  HealthStatus = "DEGRADED"
	StatusUnhealthy HealthStatus = "UNHEALTHY"
)

type Report struct {
	WorkerID  string
	Status    HealthStatus
	Error     error
	Timestamp time.Time
}

type Manager struct {
	mu        sync.RWMutex
	reports   map[string]Report
	listeners []func(Report)
}

func NewManager() *Manager {
	return &Manager{
		reports: make(map[string]Report),
	}
}

func (hm *Manager) Report(workerID string, status HealthStatus, err error) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	rep := Report{
		WorkerID:  workerID,
		Status:    status,
		Error:     err,
		Timestamp: time.Now(),
	}
	hm.reports[workerID] = rep
	for _, listener := range hm.listeners {
		listener(rep)
	}
}

func (hm *Manager) Get(workerID string) (Report, bool) {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	rep, ok := hm.reports[workerID]
	return rep, ok
}

func (hm *Manager) Subscribe(fn func(Report)) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	hm.listeners = append(hm.listeners, fn)
}

