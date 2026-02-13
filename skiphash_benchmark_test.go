package skiphash

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
)

const (
	benchUniverse   = 100_000
	benchRangeWidth = 128

	paperUniverse    = 1_000_000
	paperRangeLength = 100
)

var benchSink atomic.Int64

type benchMap interface {
	Load(int) (int, bool)
	Store(int, int)
	Delete(int)
	RangeCount(int, int) int
}

type adapter struct {
	sh *SkipHash[int, int]
}

func newAdapter() benchMap {
	return &adapter{
		sh: NewSkipHash[int, int](WithRandSource(rand.NewSource(1))),
	}
}

func (a *adapter) Load(k int) (int, bool) {
	return a.sh.Get(k)
}

func (a *adapter) Store(k, v int) {
	a.sh.Store(k, v)
}

func (a *adapter) Delete(k int) {
	a.sh.Remove(k)
}

func (a *adapter) RangeCount(low, high int) int {
	return a.sh.RangeCount(low, high)
}

type lockedMapAdapter struct {
	mu sync.RWMutex
	m  map[int]int
}

func newLockedMapAdapter() benchMap {
	return &lockedMapAdapter{m: make(map[int]int)}
}

func (a *lockedMapAdapter) Load(k int) (int, bool) {
	a.mu.RLock()
	v, ok := a.m[k]
	a.mu.RUnlock()
	return v, ok
}

func (a *lockedMapAdapter) Store(k, v int) {
	a.mu.Lock()
	a.m[k] = v
	a.mu.Unlock()
}

func (a *lockedMapAdapter) Delete(k int) {
	a.mu.Lock()
	delete(a.m, k)
	a.mu.Unlock()
}

func (a *lockedMapAdapter) RangeCount(low, high int) int {
	a.mu.RLock()
	count := 0
	for k := range a.m {
		if k >= low && k <= high {
			count++
		}
	}
	a.mu.RUnlock()
	return count
}

type syncMapAdapter struct {
	m sync.Map
}

func newSyncMapAdapter() benchMap {
	return &syncMapAdapter{}
}

func (a *syncMapAdapter) Load(k int) (int, bool) {
	v, ok := a.m.Load(k)
	if !ok {
		return 0, false
	}
	return v.(int), true
}

func (a *syncMapAdapter) Store(k, v int) {
	a.m.Store(k, v)
}

func (a *syncMapAdapter) Delete(k int) {
	a.m.Delete(k)
}

func (a *syncMapAdapter) RangeCount(low, high int) int {
	count := 0
	a.m.Range(func(key, _ any) bool {
		k := key.(int)
		if k >= low && k <= high {
			count++
		}
		return true
	})
	return count
}

var benchmarkImplementations = []struct {
	name string
	new  func() benchMap
}{
	{name: "skiphash", new: newAdapter},
	{name: "map+rwmutex", new: newLockedMapAdapter},
	{name: "sync.Map", new: newSyncMapAdapter},
}

type workloadConfig struct {
	name        string
	universe    int
	rangeLength int

	lookupPct int
	updatePct int
	rangePct  int
}

func (w workloadConfig) validate() {
	if w.universe <= 0 {
		panic(fmt.Sprintf("invalid universe for %s: %d", w.name, w.universe))
	}
	if w.rangeLength < 0 {
		panic(fmt.Sprintf("invalid range length for %s: %d", w.name, w.rangeLength))
	}
	if w.lookupPct < 0 || w.updatePct < 0 || w.rangePct < 0 {
		panic(fmt.Sprintf("invalid workload ratios for %s", w.name))
	}
	if w.lookupPct+w.updatePct+w.rangePct != 100 {
		panic(fmt.Sprintf("workload %s does not sum to 100", w.name))
	}
}

func prefill(m benchMap, universe int) {
	for k := 0; k < universe; k += 2 {
		m.Store(k, k)
	}
}

func runWorkloadOnAllMaps(b *testing.B, cfg workloadConfig) {
	cfg.validate()
	for _, impl := range benchmarkImplementations {
		impl := impl
		b.Run(impl.name, func(b *testing.B) {
			m := impl.new()
			prefill(m, cfg.universe)
			runWorkloadParallel(b, m, cfg)
		})
	}
}

func runWorkloadParallel(b *testing.B, m benchMap, cfg workloadConfig) {
	b.ReportAllocs()
	var seedCounter atomic.Uint64
	span := max(cfg.universe-cfg.rangeLength, 1)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		seed := int64(1469598103934665603 + seedCounter.Add(1))
		r := rand.New(rand.NewSource(seed))
		var local int64

		for pb.Next() {
			op := r.Intn(100)
			key := r.Intn(cfg.universe)
			switch {
			case op < cfg.lookupPct:
				if v, ok := m.Load(key); ok {
					local += int64(v)
				}
			case op < cfg.lookupPct+cfg.updatePct:
				if r.Intn(2) == 0 {
					m.Store(key, key)
				} else {
					m.Delete(key)
				}
			default:
				low := r.Intn(span)
				high := low + cfg.rangeLength
				local += int64(m.RangeCount(low, high))
			}
		}

		benchSink.Add(local)
	})
}

func BenchmarkPaperFigure5Workloads(b *testing.B) {
	workloads := []workloadConfig{
		{
			name:        "fig5a_100_lookup",
			universe:    paperUniverse,
			rangeLength: paperRangeLength,
			lookupPct:   100,
			updatePct:   0,
			rangePct:    0,
		},
		{
			name:        "fig5b_100_update",
			universe:    paperUniverse,
			rangeLength: paperRangeLength,
			lookupPct:   0,
			updatePct:   100,
			rangePct:    0,
		},
		{
			name:        "fig5c_100_range",
			universe:    paperUniverse,
			rangeLength: paperRangeLength,
			lookupPct:   0,
			updatePct:   0,
			rangePct:    100,
		},
		{
			name:        "fig5d_80_lookup_10_update_10_range",
			universe:    paperUniverse,
			rangeLength: paperRangeLength,
			lookupPct:   80,
			updatePct:   10,
			rangePct:    10,
		},
		{
			name:        "fig5e_80_update_20_range",
			universe:    paperUniverse,
			rangeLength: paperRangeLength,
			lookupPct:   0,
			updatePct:   80,
			rangePct:    20,
		},
		{
			name:        "fig5f_1_lookup_98_update_1_range",
			universe:    paperUniverse,
			rangeLength: paperRangeLength,
			lookupPct:   1,
			updatePct:   98,
			rangePct:    1,
		},
	}

	for _, cfg := range workloads {
		b.Run(cfg.name, func(b *testing.B) {
			runWorkloadOnAllMaps(b, cfg)
		})
	}
}

func BenchmarkOrderedMapReadMostlyParallel(b *testing.B) {
	cfg := workloadConfig{
		name:        "read_mostly",
		universe:    benchUniverse,
		rangeLength: benchRangeWidth,
		lookupPct:   86,
		updatePct:   12,
		rangePct:    2,
	}
	runWorkloadOnAllMaps(b, cfg)
}

func BenchmarkOrderedMapUpdateHeavyParallel(b *testing.B) {
	cfg := workloadConfig{
		name:        "update_heavy",
		universe:    benchUniverse,
		rangeLength: benchRangeWidth,
		lookupPct:   6,
		updatePct:   90,
		rangePct:    4,
	}
	runWorkloadOnAllMaps(b, cfg)
}

func BenchmarkOrderedMapRangeParallel(b *testing.B) {
	cfg := workloadConfig{
		name:        "range_only",
		universe:    benchUniverse,
		rangeLength: benchRangeWidth,
		lookupPct:   0,
		updatePct:   0,
		rangePct:    100,
	}
	runWorkloadOnAllMaps(b, cfg)
}
