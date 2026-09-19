package config

import (
	"fmt"
	"net"
	"time"
)

func ValidateAndNormalize(raw *RawConfig) (*Config, error) {
	if raw.SOCKS5.ListenAddr == "" {
		raw.SOCKS5.ListenAddr = "127.0.0.1:1080"
	}
	if _, _, err := net.SplitHostPort(raw.SOCKS5.ListenAddr); err != nil {
		return nil, fmt.Errorf("invalid socks5 listen_addr: %w", err)
	}

	payloads := make(map[string]PayloadConfig)
	for _, p := range raw.Payloads {
		if p.Name == "" {
			return nil, fmt.Errorf("payload name cannot be empty")
		}
		payloads[p.Name] = PayloadConfig{
			Name: p.Name,
			Data: p.Data,
		}
	}

	workers := make(map[string]WorkerConfig)
	for _, w := range raw.Workers {
		if w.ID == "" {
			return nil, fmt.Errorf("worker ID cannot be empty")
		}
		if _, exists := workers[w.ID]; exists {
			return nil, fmt.Errorf("duplicate worker ID: %s", w.ID)
		}

		timeout := 10 * time.Second
		if w.ConnectTimeout != "" {
			parsed, err := time.ParseDuration(w.ConnectTimeout)
			if err != nil {
				return nil, fmt.Errorf("worker %s invalid connect_timeout: %w", w.ID, err)
			}
			timeout = parsed
		}

		payloadData := ""
		if w.PayloadName != "" {
			if p, ok := payloads[w.PayloadName]; ok {
				payloadData = p.Data
			} else {
				return nil, fmt.Errorf("worker %s references unknown payload: %s", w.ID, w.PayloadName)
			}
		}

		// Normalisasi mode
		mode := w.RemoteProxyMode
		if mode == "" {
			mode = "http"
		}

		path := w.RemoteProxyPath
		if path == "" {
			path = "/"
		}

		workers[w.ID] = WorkerConfig{
			ID:              w.ID,
			Type:            w.Type,
			Host:            w.Host,
			Port:            w.Port,
			Username:        w.Username,
			Password:        w.Password,
			PrivateKey:      w.PrivateKey,
			PayloadName:     w.PayloadName,
			PayloadData:     payloadData,
			RemoteProxy:     w.RemoteProxy,
			RemoteProxyMode: mode,
			RemoteProxyPath: path,
			RemoteProxyTLS:  w.RemoteProxyTLS,
			ConnectTimeout:  timeout,
			KeepAliveSec:    time.Duration(w.KeepAliveSec) * time.Second,
		}
	}

	outbounds := make(map[string]OutboundConfig)
	for _, o := range raw.Outbounds {
		if o.Name == "" {
			return nil, fmt.Errorf("outbound name cannot be empty")
		}
		if _, exists := outbounds[o.Name]; exists {
			return nil, fmt.Errorf("duplicate outbound name: %s", o.Name)
		}
		outbounds[o.Name] = OutboundConfig{
			Name:      o.Name,
			Type:      o.Type,
			WorkerIDs: o.WorkerIDs,
			Strategy:  o.Strategy,
		}
	}

	domains := make(map[string]DomainConfig)
	for _, d := range raw.Domains {
		if d.Name == "" {
			return nil, fmt.Errorf("domain entry name cannot be empty")
		}
		domains[d.Name] = DomainConfig{
			Name:     d.Name,
			FilePath: d.FilePath,
		}
	}

	rules := make([]RuleConfig, 0, len(raw.Rules))
	for _, r := range raw.Rules {
		rules = append(rules, RuleConfig{
			Type:     r.Type,
			Value:    r.Value,
			Outbound: r.Outbound,
		})
	}

	return &Config{
		App: AppConfig{
			LogLevel: raw.App.LogLevel,
			LogType:  raw.App.LogType,
			APIAddr:  raw.App.APIAddr,
		},
		SOCKS5: SOCKS5Config{
			ListenAddr: raw.SOCKS5.ListenAddr,
			Username:   raw.SOCKS5.Username,
			Password:   raw.SOCKS5.Password,
		},
		Workers:   workers,
		Outbounds: outbounds,
		Rules:     rules,
		Domains:   domains,
		Payloads:  payloads,
	}, nil
}