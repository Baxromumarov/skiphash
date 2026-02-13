package skiphash

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSkipHashInsertGetRemove(t *testing.T) {
	sh := NewSkipHash[int, string](WithRandSource(rand.NewSource(1)))

	assert.True(t, sh.Insert(10, "a"), "expected first insert to succeed")
	assert.False(t, sh.Insert(10, "b"), "expected duplicate insert to fail")

	got, ok := sh.Get(10)
	assert.True(t, ok, "expected get to succeed")
	assert.Equal(t, "a", got, "unexpected get value")

	assert.True(t, sh.Remove(10), "expected remove to succeed")
	assert.False(t, sh.Remove(10), "expected second remove to fail")
	_, ok = sh.Get(10)
	assert.False(t, ok, "expected key to be absent after removal")
	assert.Equal(t, 0, sh.Len(), "expected len=0 after removal")
}

func TestSkipHashReinsertAfterLogicalDelete(t *testing.T) {
	sh := NewSkipHash[int, string](WithRandSource(rand.NewSource(2)))

	assert.True(t, sh.Insert(7, "old"), "insert old failed")
	assert.True(t, sh.Remove(7), "remove old failed")
	assert.True(t, sh.Insert(7, "new"), "reinsert failed")

	got, ok := sh.Get(7)
	assert.True(t, ok, "expected value after reinsert")
	assert.Equal(t, "new", got, "unexpected value after reinsert")

	entries := sh.Range(7, 7)
	assert.Len(t, entries, 1, "expected single live entry in range")
	assert.Equal(t, "new", entries[0].Value, "unexpected range value")
}

func TestSkipHashRangeAndPointQueries(t *testing.T) {
	sh := NewSkipHash[int, int](WithRandSource(rand.NewSource(3)))
	for _, k := range []int{5, 1, 3, 2, 4, 8, 6} {
		assert.True(t, sh.Insert(k, k*10), "insert failed for key=%d", k)
	}
	assert.True(t, sh.Remove(3), "remove failed for key=3")

	entries := sh.Range(2, 6)
	gotKeys := make([]int, 0, len(entries))
	for _, e := range entries {
		gotKeys = append(gotKeys, e.Key)
	}
	wantKeys := []int{2, 4, 5, 6}
	assert.Equal(t, wantKeys, gotKeys, "unexpected range keys")

	ceil, ok := sh.Ceil(3)
	assert.True(t, ok, "unexpected ceil(3)")
	assert.Equal(t, 4, ceil.Key, "unexpected ceil(3) key")

	succ, ok := sh.Succ(5)
	assert.True(t, ok, "unexpected succ(5)")
	assert.Equal(t, 6, succ.Key, "unexpected succ(5) key")

	floor, ok := sh.Floor(3)
	assert.True(t, ok, "unexpected floor(3)")
	assert.Equal(t, 2, floor.Key, "unexpected floor(3) key")

	pred, ok := sh.Pred(5)
	assert.True(t, ok, "unexpected pred(5)")
	assert.Equal(t, 4, pred.Key, "unexpected pred(5) key")
}

func TestSkipHashConcurrentSanity(t *testing.T) {
	sh := NewSkipHash[int, int](WithRandSource(rand.NewSource(4)))
	const (
		workers  = 8
		opsPerG  = 5000
		universe = 2048
	)

	var wg sync.WaitGroup
	for w := range workers {
		seed := int64(100 + w)
		wg.Go(func() {
			r := rand.New(rand.NewSource(seed))
			for range opsPerG {
				k := r.Intn(universe)
				switch r.Intn(4) {
				case 0:
					sh.Store(k, k)
				case 1:
					sh.Remove(k)
				case 2:
					sh.Get(k)
				default:
					low := r.Intn(universe)
					high := low + r.Intn(32)
					sh.Range(low, high)
				}
			}
		})
	}
	wg.Wait()

	assert.GreaterOrEqual(t, sh.Len(), 0, "len should never be negative")
}

func TestSkipHashRangeCount(t *testing.T) {
	sh := NewSkipHash[int, int](WithRandSource(rand.NewSource(time.Now().UnixNano())))
	for i := range 100 {
		sh.Store(i, i)
	}
	for i := 10; i < 20; i++ {
		sh.Remove(i)
	}

	got := sh.RangeCount(0, 99)
	assert.Equal(t, 90, got, "unexpected range count")
}
