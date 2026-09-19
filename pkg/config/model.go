package config

import (
	"time"
)

type Config struct {
	App       AppConfig
	SOCKS5    SOCKS5Config
	Workers   map[string]WorkerConfig
	Outbounds map[string]OutboundConfig
	Rules     []RuleConfig
	Domains   map[string]DomainConfig
	Payloads  map[string]PayloadConfig
}

type AppConfig struct {
	LogLevel string
	LogType  string
	APIAddr  string
}

type SOCKS5Config struct {
	ListenAddr string
	Username   string
	Password   string
}

type WorkerConfig struct {
	ID             string
	Type           string
	Host           string
	Port           int
	Username       string
	Password       string
	PrivateKey     string
	PayloadName    string
	ConnectTimeout time.Duration
	KeepAliveSec   time.Duration
}

type OutboundConfig struct {
	Name      string
	Type      string
	WorkerIDs []string
	Strategy  string
}

type RuleConfig struct {
	Type     string
	Value    string
	Outbound string
}

type DomainConfig struct {
	Name     string
	FilePath string
}

type PayloadConfig struct {
	Name string
	Data string
}

