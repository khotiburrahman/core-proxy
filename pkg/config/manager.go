package config

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"gopkg.in/yaml.v3"
)

type Manager struct {
	current atomic.Pointer[Config]
	mu      sync.Mutex
	path    string
}

func NewManager(filePath string) (*Manager, error) {
	mgr := &Manager{path: filePath}
	if err := mgr.LoadFromFile(filePath); err != nil {
		return nil, err
	}
	return mgr, nil
}

func (m *Manager) LoadFromFile(filePath string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	var raw RawConfig
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("failed to parse YAML: %w", err)
	}

	cfg, err := ValidateAndNormalize(&raw)
	if err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.current.Store(cfg)
	m.path = filePath
	return nil
}

func (m *Manager) Get() *Config {
	return m.current.Load()
}

// LoadConfig membaca file dan mengembalikan Config yang sudah divalidasi.
func LoadConfig(filePath string) (*Config, error) {
	mgr, err := NewManager(filePath)
	if err != nil {
		return nil, err
	}
	return mgr.Get(), nil
}

