package config

type RawConfig struct {
	App       RawAppConfig        `yaml:"app"`
	SOCKS5    RawSOCKS5Config     `yaml:"socks5"`
	Workers   []RawWorkerConfig   `yaml:"workers"`
	Outbounds []RawOutboundConfig `yaml:"outbounds"`
	Rules     []RawRuleConfig     `yaml:"rules"`
	Domains   []RawDomainConfig   `yaml:"domains"`
	Payloads  []RawPayloadConfig  `yaml:"payloads"`
}

type RawAppConfig struct {
	LogLevel string `yaml:"log_level"`
	LogType  string `yaml:"log_type"`
	APIAddr  string `yaml:"api_addr"`
}

type RawSOCKS5Config struct {
	ListenAddr string `yaml:"listen_addr"`
	Username   string `yaml:"username"`
	Password   string `yaml:"password"`
}

type RawWorkerConfig struct {
	ID             string `yaml:"id"`
	Type           string `yaml:"type"`
	Host           string `yaml:"host"`
	Port           int    `yaml:"port"`
	Username       string `yaml:"username"`
	Password       string `yaml:"password"`
	PrivateKey     string `yaml:"private_key"`
	PayloadName    string `yaml:"payload_name"`
	RemoteProxy    string `yaml:"remote_proxy"`
	ConnectTimeout string `yaml:"connect_timeout"`
	KeepAliveSec   int    `yaml:"keepalive_sec"`
}

type RawOutboundConfig struct {
	Name      string   `yaml:"name"`
	Type      string   `yaml:"type"`
	WorkerIDs []string `yaml:"worker_ids"`
	Strategy  string   `yaml:"strategy"`
}

type RawRuleConfig struct {
	Type     string `yaml:"type"`
	Value    string `yaml:"value"`
	Outbound string `yaml:"outbound"`
}

type RawDomainConfig struct {
	Name     string `yaml:"name"`
	FilePath string `yaml:"file_path"`
}

type RawPayloadConfig struct {
	Name string `yaml:"name"`
	Data string `yaml:"data"`
}