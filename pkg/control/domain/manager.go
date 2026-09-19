package domain

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync/atomic"

	"core-proxy/pkg/common/observability"
)

type ListMatcher struct {
	trie     *DomainTrie
	keywords map[string]string
}

type Manager struct {
	activeMatcher atomic.Pointer[ListMatcher]
}

func NewManager() *Manager {
	mgr := &Manager{}
	matcher := &ListMatcher{
		trie:     NewDomainTrie(),
		keywords: make(map[string]string),
	}
	mgr.activeMatcher.Store(matcher)
	return mgr
}

func (m *Manager) LoadListFile(filePath string, targetOutbound string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open domain list file %s: %w", filePath, err)
	}
	defer file.Close()

	newTrie := NewDomainTrie()
	newKeywords := make(map[string]string)

	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		if strings.HasPrefix(line, "keyword:") {
			kw := strings.TrimPrefix(line, "keyword:")
			newKeywords[strings.ToLower(kw)] = targetOutbound
		} else {
			newTrie.Insert(line, targetOutbound)
		}
		count++
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading domain list %s: %w", filePath, err)
	}

	newMatcher := &ListMatcher{
		trie:     newTrie,
		keywords: newKeywords,
	}
	m.activeMatcher.Store(newMatcher)

	observability.Info("Domain list loaded successfully", "file", filePath, "records", count, "target", targetOutbound)
	return nil
}

func (m *Manager) Match(domain string) (string, bool) {
	matcher := m.activeMatcher.Load()
	if matcher == nil {
		return "", false
	}

	if target, ok := matcher.trie.Match(domain); ok {
		return target, true
	}

	domainLower := strings.ToLower(domain)
	for kw, target := range matcher.keywords {
		if strings.Contains(domainLower, kw) {
			return target, true
		}
	}

	return "", false
}

