package domain

import (
	"strings"
	"sync"
)

type TrieNode struct {
	children map[string]*TrieNode
	target   string
	isEnd    bool
}

func NewTrieNode() *TrieNode {
	return &TrieNode{
		children: make(map[string]*TrieNode),
	}
}

type DomainTrie struct {
	root *TrieNode
	mu   sync.RWMutex
}

func NewDomainTrie() *DomainTrie {
	return &DomainTrie{
		root: NewTrieNode(),
	}
}

func (dt *DomainTrie) Insert(domain string, target string) {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	domain = strings.ToLower(strings.TrimSpace(domain))
	if domain == "" {
		return
	}

	labels := strings.Split(domain, ".")
	curr := dt.root

	for i := len(labels) - 1; i >= 0; i-- {
		label := labels[i]
		if _, exists := curr.children[label]; !exists {
			curr.children[label] = NewTrieNode()
		}
		curr = curr.children[label]
	}

	curr.isEnd = true
	curr.target = target
}

func (dt *DomainTrie) Match(domain string) (string, bool) {
	dt.mu.RLock()
	defer dt.mu.RUnlock()

	domain = strings.ToLower(strings.TrimSpace(domain))
	labels := strings.Split(domain, ".")
	curr := dt.root

	var lastTarget string
	var found bool

	for i := len(labels) - 1; i >= 0; i-- {
		label := labels[i]
		next, exists := curr.children[label]
		if !exists {
			break
		}
		curr = next
		if curr.isEnd {
			lastTarget = curr.target
			found = true
		}
	}

	return lastTarget, found
}

