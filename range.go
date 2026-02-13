package skiphash

import (
	"runtime"
)

func (sh *SkipHash[K, V]) Range(low, high K) []Entry[K, V] {
	if low > high {
		return nil
	}
	if entries, ok := sh.rangeFast(low, high); ok {
		return entries
	}
	return sh.rangeSlow(low, high)
}

func (sh *SkipHash[K, V]) rangeFast(low, high K) ([]Entry[K, V], bool) {
	for try := 0; try < sh.fastPathTries; try++ {
		if !sh.mu.TryRLock() {
			runtime.Gosched()
			continue
		}
		entries := make([]Entry[K, V], 0, 16)
		for node := sh.lowerBoundLocked(low); node != sh.tail && node.key <= high; node = node.next[0] {
			if node.rTime == 0 {
				entries = append(entries, Entry[K, V]{Key: node.key, Value: node.value})
			}
		}
		sh.mu.RUnlock()
		return entries, true
	}
	return nil, false
}

func (sh *SkipHash[K, V]) rangeSlow(low, high K) []Entry[K, V] {
	var (
		start *slNode[K, V]
		ver   uint64
	)

	sh.mu.Lock()
	start = sh.firstLiveGELocked(low)
	ver = sh.rqc.onRangeLocked()
	sh.mu.Unlock()

	entries := make([]Entry[K, V], 0, 16)
	node := start
	for {
		sh.mu.RLock()
		if node == sh.tail || node.key > high {
			sh.mu.RUnlock()
			break
		}

		include := node != sh.head &&
			node.iTime < ver &&
			(node.rTime == 0 || node.rTime >= ver)
		next := sh.nextSafeLocked(node, ver)
		key := node.key
		value := node.value
		sh.mu.RUnlock()

		if include {
			entries = append(entries, Entry[K, V]{Key: key, Value: value})
		}
		node = next
	}

	sh.mu.Lock()
	sh.rqc.afterRangeLocked(sh, ver)
	sh.mu.Unlock()

	return entries
}

func (sh *SkipHash[K, V]) nextSafeLocked(node *slNode[K, V], ver uint64) *slNode[K, V] {
	next := node.next[0]
	for next != sh.tail && !sh.isSafeLocked(next, ver) {
		next = next.next[0]
	}
	return next
}

func (sh *SkipHash[K, V]) isSafeLocked(node *slNode[K, V], ver uint64) bool {
	if node == sh.head || node == sh.tail {
		return true
	}
	if node.iTime >= ver {
		return false
	}
	return node.rTime == 0 || node.rTime >= ver
}
