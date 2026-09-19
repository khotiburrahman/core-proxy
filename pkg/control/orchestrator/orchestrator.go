package orchestrator

import (
	"context"
	"fmt"
	"sync"

	"core-proxy/pkg/common/observability"
	"core-proxy/pkg/config"
	"core-proxy/pkg/control/domain"
	"core-proxy/pkg/control/outbound"
	"core-proxy/pkg/control/rule"
	"core-proxy/pkg/control/worker"
)

type Orchestrator struct {
	configPath    string
	mu            sync.Mutex
	workerMgr     *worker.Manager
	outboundMgr   *outbound.Manager
	ruleEngine    *rule.Engine
	domainListMgr *domain.Manager
	ctx           context.Context
}

func NewOrchestrator(
	cfgPath string,
	wMgr *worker.Manager,
	oMgr *outbound.Manager,
	rEngine *rule.Engine,
	dMgr *domain.Manager,
) *Orchestrator {
	return &Orchestrator{
		configPath:    cfgPath,
		workerMgr:     wMgr,
		outboundMgr:   oMgr,
		ruleEngine:    rEngine,
		domainListMgr: dMgr,
	}
}

func (o *Orchestrator) Reload(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	observability.Info("Initiating zero-downtime hot reload", "path", o.configPath)

	newCfg, err := config.LoadConfig(o.configPath)
	if err != nil {
		return fmt.Errorf("reload aborted, invalid config: %w", err)
	}

	o.workerMgr.SyncWithConfig(ctx, newCfg.Workers)
	o.outboundMgr.SyncWithConfig(newCfg.Outbounds)

	if err := o.ruleEngine.CompileAndApply(newCfg.Rules); err != nil {
		return fmt.Errorf("failed compiling rules: %w", err)
	}

	for _, d := range newCfg.Domains {
		if err := o.domainListMgr.LoadListFile(d.FilePath, d.Name); err != nil {
			observability.Warn("Failed to load domain list", "name", d.Name, "path", d.FilePath, "err", err)
		}
	}

	observability.Info("Zero-downtime hot reload completed successfully")
	return nil
}

