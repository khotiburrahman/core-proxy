package main

import (
	"context"
	"flag"
	"net"
	"time"

	"core-proxy/pkg/common/observability"
	"core-proxy/pkg/common/shutdown"
	"core-proxy/pkg/config"
	"core-proxy/pkg/control/api"
	"core-proxy/pkg/control/domain"
	"core-proxy/pkg/control/health"
	"core-proxy/pkg/control/orchestrator"
	"core-proxy/pkg/control/outbound"
	"core-proxy/pkg/control/rule"
	"core-proxy/pkg/control/worker"
	"core-proxy/pkg/dataplane/socks5"
)

func main() {
	cfgPath := flag.String("config", "config.yaml", "Path to config file")
	flag.Parse()

	observability.InitLogger("info")
	observability.Info("Starting Core Proxy Engine", "config", *cfgPath)

	cfg, err := config.LoadConfig(*cfgPath)
	if err != nil {
		observability.Fatal("Failed to load initial configuration", "err", err)
	}

	observability.Init(observability.Config{
		Level:  cfg.App.LogLevel,
		Format: cfg.App.LogType,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	healthMgr := health.NewManager()
	workerMgr := worker.NewManager(healthMgr)
	outboundMgr := outbound.NewManager(workerMgr)
	domainMgr := domain.NewManager()
	ruleEngine := rule.NewEngine(domainMgr)

	orch := orchestrator.NewOrchestrator(*cfgPath, workerMgr, outboundMgr, ruleEngine, domainMgr)

	if err := orch.Reload(ctx); err != nil {
		observability.Fatal("Failed initial orchestration setup", "err", err)
	}

	socksDialer := &outboundDialerAdapter{
		outboundMgr: outboundMgr,
		ruleEngine:  ruleEngine,
	}

	socksServer := socks5.NewServer(
		cfg.SOCKS5.ListenAddr,
		socks5.ServerConfig{
			Username: cfg.SOCKS5.Username,
			Password: cfg.SOCKS5.Password,
		},
		socksDialer,
	)
	if err := socksServer.Start(ctx); err != nil {
		observability.Fatal("Failed to start SOCKS5 Server", "err", err)
	}

	apiSrv := api.NewServer(
		cfg.App.APIAddr,
		healthMgr,
		workerMgr,
		func() error { return orch.Reload(ctx) },
	)
	_ = apiSrv.Start()

	coordinator := shutdown.NewCoordinator(15 * time.Second)
	coordinator.Register(func(c context.Context) {
		observability.Info("Stopping API Server...")
		_ = apiSrv.Stop(c)
	})
	coordinator.Register(func(c context.Context) {
		observability.Info("Stopping SOCKS5 Data Plane Engine...")
		socksServer.Stop()
	})
	coordinator.Register(func(c context.Context) {
		observability.Info("Stopping SSH Worker Connections...")
		workerMgr.StopAll()
	})

	coordinator.WaitAndShutdown()
}

type outboundDialerAdapter struct {
	outboundMgr *outbound.Manager
	ruleEngine  *rule.Engine
}

func (a *outboundDialerAdapter) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	outboundTarget := a.ruleEngine.Match(address)
	return a.outboundMgr.DialContext(ctx, outboundTarget, network, address)
}
