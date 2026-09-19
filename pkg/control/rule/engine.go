package rule

import (
	"net"
	"strings"
	"sync/atomic"

	"core-proxy/pkg/common/observability"
	"core-proxy/pkg/config"
	"core-proxy/pkg/control/domain"
)

type RuleType string

const (
	TypeDomain        RuleType = "domain"
	TypeDomainSuffix  RuleType = "domain_suffix"
	TypeDomainKeyword RuleType = "domain_keyword"
	TypeCIDR          RuleType = "cidr"
	TypeMatch         RuleType = "match"
)

type CompiledRule struct {
	Type     RuleType
	Value    string
	Outbound string
	ipNet    *net.IPNet
}

type RuntimeRuleSet struct {
	exactDomains map[string]string
	domainTrie   *domain.DomainTrie
	keywords     map[string]string
	cidrRules    []CompiledRule
	matchDefault string
}

type Engine struct {
	activeRules   atomic.Pointer[RuntimeRuleSet]
	domainListMgr *domain.Manager
}

func NewEngine(domainListMgr *domain.Manager) *Engine {
	e := &Engine{domainListMgr: domainListMgr}
	empty := &RuntimeRuleSet{
		exactDomains: make(map[string]string),
		domainTrie:   domain.NewDomainTrie(),
		keywords:     make(map[string]string),
		matchDefault: "DIRECT",
	}
	e.activeRules.Store(empty)
	return e
}

func (e *Engine) CompileAndApply(ruleCfgs []config.RuleConfig) error {
	newSet := &RuntimeRuleSet{
		exactDomains: make(map[string]string),
		domainTrie:   domain.NewDomainTrie(),
		keywords:     make(map[string]string),
		matchDefault: "DIRECT",
	}

	for _, rc := range ruleCfgs {
		rType := RuleType(strings.ToLower(rc.Type))
		val := strings.ToLower(strings.TrimSpace(rc.Value))

		switch rType {
		case TypeDomain:
			newSet.exactDomains[val] = rc.Outbound
		case TypeDomainSuffix:
			newSet.domainTrie.Insert(val, rc.Outbound)
		case TypeDomainKeyword:
			newSet.keywords[val] = rc.Outbound
		case TypeCIDR:
			_, ipNet, err := net.ParseCIDR(rc.Value)
			if err != nil {
				observability.Error("Invalid CIDR rule skipped", "value", rc.Value, "err", err)
				continue
			}
			newSet.cidrRules = append(newSet.cidrRules, CompiledRule{
				Type:     TypeCIDR,
				Value:    rc.Value,
				Outbound: rc.Outbound,
				ipNet:    ipNet,
			})
		case TypeMatch:
			newSet.matchDefault = rc.Outbound
		}
	}

	e.activeRules.Store(newSet)
	observability.Info("Rule engine re-compiled and applied atomically", "total_rules", len(ruleCfgs))
	return nil
}

func (e *Engine) Match(host string) string {
	ruleSet := e.activeRules.Load()
	if ruleSet == nil {
		return "DIRECT"
	}

	hostLower := strings.ToLower(strings.TrimSpace(host))

	if ip := net.ParseIP(hostLower); ip != nil {
		for _, cr := range ruleSet.cidrRules {
			if cr.ipNet.Contains(ip) {
				return cr.Outbound
			}
		}
		return ruleSet.matchDefault
	}

	if outbound, ok := ruleSet.exactDomains[hostLower]; ok {
		return outbound
	}

	if e.domainListMgr != nil {
		if outbound, ok := e.domainListMgr.Match(hostLower); ok {
			return outbound
		}
	}

	if outbound, ok := ruleSet.domainTrie.Match(hostLower); ok {
		return outbound
	}

	for kw, outbound := range ruleSet.keywords {
		if strings.Contains(hostLower, kw) {
			return outbound
		}
	}

	return ruleSet.matchDefault
}

