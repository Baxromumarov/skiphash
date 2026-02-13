package skiphash

import (
	"cmp"
	"math/rand"
	"sync"
	"time"
)

const (
	DefaultMaxLevel      = 20
	DefaultFastPathTries = 3
)

type Option func(*config)

type config struct {
	maxLevel      int
	fastPathTries int
	randSource    rand.Source
}

func WithMaxLevel(level int) Option {
	return func(cfg *config) {
		if level > 0 {
			cfg.maxLevel = level
		}
	}
}

func WithFastPathTries(tries int) Option {
	return func(cfg *config) {
		if tries >= 0 {
			cfg.fastPathTries = tries
		}
	}
}

func WithRandSource(source rand.Source) Option {
	return func(cfg *config) {
		if source != nil {
			cfg.randSource = source
		}
	}
}

type Entry[K cmp.Ordered, V any] struct {
	Key   K
	Value V
}

type SkipHash[K cmp.Ordered, V any] struct {
	mu sync.RWMutex

	maxLevel      int
	fastPathTries int
	rng           *rand.Rand

	index map[K]*slNode[K, V]
	head  *slNode[K, V]
	tail  *slNode[K, V]
	len   int

	rqc *rangeCoordinator[K, V]
}

type slNode[K cmp.Ordered, V any] struct {
	key    K
	value  V
	height uint8

	prev []*slNode[K, V]
	next []*slNode[K, V]

	// iTime / rTime match the paper:
	// - iTime: range version visible at insertion
	// - rTime: 0 means logically present, otherwise logical removal version
	iTime uint64
	rTime uint64

	unstitched bool
}

func New[K cmp.Ordered, V any](opts ...Option) *SkipHash[K, V] {
	cfg := config{
		maxLevel:      DefaultMaxLevel,
		fastPathTries: DefaultFastPathTries,
		randSource:    rand.NewSource(time.Now().UnixNano()),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.maxLevel <= 0 {
		cfg.maxLevel = DefaultMaxLevel
	}
	if cfg.fastPathTries < 0 {
		cfg.fastPathTries = DefaultFastPathTries
	}
	if cfg.randSource == nil {
		cfg.randSource = rand.NewSource(time.Now().UnixNano())
	}

	head := newSentinel[K, V](uint8(cfg.maxLevel))
	tail := newSentinel[K, V](uint8(cfg.maxLevel))
	for level := uint8(0); level < uint8(cfg.maxLevel); level++ {
		head.next[level] = tail
		tail.prev[level] = head
	}

	return &SkipHash[K, V]{
		maxLevel:      cfg.maxLevel,
		fastPathTries: cfg.fastPathTries,
		rng:           rand.New(cfg.randSource),
		index:         make(map[K]*slNode[K, V]),
		head:          head,
		tail:          tail,
		rqc:           newRangeCoordinator[K, V](),
	}
}

func newSentinel[K cmp.Ordered, V any](height uint8) *slNode[K, V] {
	return &slNode[K, V]{
		height: height,
		prev:   make([]*slNode[K, V], height),
		next:   make([]*slNode[K, V], height),
	}
}

func (sh *SkipHash[K, V]) Len() int {
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	return sh.len
}

func (sh *SkipHash[K, V]) Get(key K) (V, bool) {
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	node, ok := sh.index[key]
	if !ok {
		var zero V
		return zero, false
	}
	return node.value, true
}

func (sh *SkipHash[K, V]) Contains(key K) bool {
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	_, ok := sh.index[key]
	return ok
}

// Insert adds a new key/value pair and fails if a key already exists.
func (sh *SkipHash[K, V]) Insert(key K, value V) bool {
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if _, exists := sh.index[key]; exists {
		return false
	}

	node := sh.insertNodeLocked(key, value)
	sh.index[key] = node
	sh.len++
	return true
}

// Store inserts or replaces the value for key.
// It returns true if a new key was inserted.
func (sh *SkipHash[K, V]) Store(key K, value V) bool {
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if node, exists := sh.index[key]; exists {
		node.value = value
		return false
	}

	node := sh.insertNodeLocked(key, value)
	sh.index[key] = node
	sh.len++
	return true
}

func (sh *SkipHash[K, V]) insertNodeLocked(key K, value V) *slNode[K, V] {
	level := sh.randomLevelLocked()
	preds, succs := sh.findInsertNeighborsLocked(key)
	node := &slNode[K, V]{
		key:    key,
		value:  value,
		height: level,
		prev:   make([]*slNode[K, V], level),
		next:   make([]*slNode[K, V], level),
		iTime:  sh.rqc.onUpdateLocked(),
	}

	for i := uint8(0); i < level; i++ {
		pred := preds[i]
		succ := succs[i]
		node.prev[i] = pred
		node.next[i] = succ
		pred.next[i] = node
		succ.prev[i] = node
	}

	return node
}

func (sh *SkipHash[K, V]) Remove(key K) bool {
	sh.mu.Lock()
	defer sh.mu.Unlock()

	node, exists := sh.index[key]
	if !exists {
		return false
	}

	delete(sh.index, key)
	node.rTime = sh.rqc.onUpdateLocked()
	sh.rqc.afterRemoveLocked(sh, node)
	sh.len--
	return true
}

func (sh *SkipHash[K, V]) Ceil(key K) (Entry[K, V], bool) {
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	if node, exists := sh.index[key]; exists {
		return Entry[K, V]{
			Key:   node.key,
			Value: node.value,
		}, true
	}

	node := sh.firstLiveGELocked(key)
	if node == sh.tail {
		var zero Entry[K, V]
		return zero, false
	}
	return Entry[K, V]{
		Key:   node.key,
		Value: node.value,
	}, true
}

func (sh *SkipHash[K, V]) Succ(key K) (Entry[K, V], bool) {
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	var node *slNode[K, V]
	if cur, exists := sh.index[key]; exists {
		node = cur.next[0]
	} else {
		node = sh.lowerBoundLocked(key)
	}
	for node != sh.tail &&
		(node.key == key || node.rTime != 0) {
		node = node.next[0]
	}

	if node == sh.tail {
		var zero Entry[K, V]
		return zero, false
	}
	return Entry[K, V]{
		Key:   node.key,
		Value: node.value,
	}, true
}

func (sh *SkipHash[K, V]) Floor(key K) (Entry[K, V], bool) {
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	if node, exists := sh.index[key]; exists {
		return Entry[K, V]{
			Key:   node.key,
			Value: node.value,
		}, true
	}

	node := sh.predecessorLocked(key, false)
	if node == sh.head {
		var zero Entry[K, V]
		return zero, false
	}
	return Entry[K, V]{
		Key:   node.key,
		Value: node.value,
	}, true
}

func (sh *SkipHash[K, V]) Pred(key K) (Entry[K, V], bool) {
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	node := sh.predecessorLocked(key, true)
	if node == sh.head {
		var zero Entry[K, V]
		return zero, false
	}
	return Entry[K, V]{
		Key:   node.key,
		Value: node.value,
	}, true
}

func (sh *SkipHash[K, V]) predecessorLocked(key K, strict bool) *slNode[K, V] {
	cur := sh.head
	for level := sh.maxLevel - 1; level >= 0; level-- {
		next := cur.next[level]
		for next != sh.tail {
			if strict {
				if next.key >= key {
					break
				}
			} else if next.key > key {
				break
			}
			cur = next
			next = cur.next[level]
		}
	}
	for cur != sh.head && cur.rTime != 0 {
		cur = cur.prev[0]
	}
	return cur
}

// RangeAll returns all logically present entries.
func (sh *SkipHash[K, V]) RangeAll() []Entry[K, V] {
	out := make([]Entry[K, V], 0, sh.Len())
	for node := sh.head.next[0]; node != sh.tail; node = node.next[0] {
		if node.rTime == 0 {
			out = append(out, Entry[K, V]{
				Key:   node.key,
				Value: node.value,
			})
		}
	}
	return out

}

// RangeCount returns how many logically present keys are in [low, high].
func (sh *SkipHash[K, V]) RangeCount(low, high K) int {
	if low > high {
		return 0
	}
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	count := 0
	for node := sh.lowerBoundLocked(low); node != sh.tail && node.key <= high; node = node.next[0] {
		if node.rTime == 0 {
			count++
		}
	}
	return count
}

func (sh *SkipHash[K, V]) lowerBoundLocked(key K) *slNode[K, V] {
	cur := sh.head
	for level := sh.maxLevel - 1; level >= 0; level-- {
		next := cur.next[level]
		for next != sh.tail && next.key < key {
			cur = next
			next = cur.next[level]
		}
	}
	return cur.next[0]
}

func (sh *SkipHash[K, V]) firstLiveGELocked(key K) *slNode[K, V] {
	for node := sh.lowerBoundLocked(key); node != sh.tail; node = node.next[0] {
		if node.rTime == 0 {
			return node
		}
	}
	return sh.tail
}

func (sh *SkipHash[K, V]) findInsertNeighborsLocked(key K) ([]*slNode[K, V], []*slNode[K, V]) {
	preds := make([]*slNode[K, V], sh.maxLevel)
	succs := make([]*slNode[K, V], sh.maxLevel)

	cur := sh.head
	for level := sh.maxLevel - 1; level >= 0; level-- {
		next := cur.next[level]
		for next != sh.tail {
			if next.key < key {
				cur = next
				next = cur.next[level]
				continue
			}
			// Reinsertions may race with deferred physical removal. We keep new
			// key instances after the logically deleted chain for the same key.
			if next.key == key && next.rTime != 0 {
				cur = next
				next = cur.next[level]
				continue
			}
			break
		}
		preds[level] = cur
		succs[level] = next
	}

	return preds, succs
}

func (sh *SkipHash[K, V]) randomLevelLocked() uint8 {
	level := 1
	for level < sh.maxLevel && sh.rng.Float64() < 0.5 {
		level++
	}
	return uint8(level)
}

func (sh *SkipHash[K, V]) unstitchNodeLocked(node *slNode[K, V]) {
	if node == nil ||
		node == sh.head ||
		node == sh.tail ||
		node.unstitched {
		return
	}
	for level := uint8(0); level < node.height; level++ {
		pred := node.prev[level]
		succ := node.next[level]
		if pred != nil {
			pred.next[level] = succ
		}
		if succ != nil {
			succ.prev[level] = pred
		}
	}
	node.unstitched = true
}
