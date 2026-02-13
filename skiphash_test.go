package skiphash

import (
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestSkipHashInsertGetRemove(t *testing.T) {
	sh := NewSkipHash[int, string](WithRandSource(rand.NewSource(1)))

	if !sh.Insert(10, "a") {
		t.Fatalf("expected first insert to succeed")
	}
	if sh.Insert(10, "b") {
		t.Fatalf("expected duplicate insert to fail")
	}

	got, ok := sh.Get(10)
	if !ok || got != "a" {
		t.Fatalf("unexpected get result: ok=%v value=%q", ok, got)
	}

	if !sh.Remove(10) {
		t.Fatalf("expected remove to succeed")
	}
	if sh.Remove(10) {
		t.Fatalf("expected second remove to fail")
	}
	if _, ok := sh.Get(10); ok {
		t.Fatalf("expected key to be absent after removal")
	}
	if sh.Len() != 0 {
		t.Fatalf("expected len=0, got %d", sh.Len())
	}
}

func TestSkipHashReinsertAfterLogicalDelete(t *testing.T) {
	sh := NewSkipHash[int, string](WithRandSource(rand.NewSource(2)))

	if !sh.Insert(7, "old") {
		t.Fatalf("insert old failed")
	}
	if !sh.Remove(7) {
		t.Fatalf("remove old failed")
	}
	if !sh.Insert(7, "new") {
		t.Fatalf("reinsert failed")
	}

	got, ok := sh.Get(7)
	if !ok || got != "new" {
		t.Fatalf("unexpected value after reinsert: ok=%v value=%q", ok, got)
	}

	entries := sh.Range(7, 7)
	if len(entries) != 1 {
		t.Fatalf("expected single live entry in range, got %d", len(entries))
	}
	if entries[0].Value != "new" {
		t.Fatalf("unexpected range value: %q", entries[0].Value)
	}
}

func TestSkipHashRangeAndPointQueries(t *testing.T) {
	sh := NewSkipHash[int, int](WithRandSource(rand.NewSource(3)))
	for _, k := range []int{5, 1, 3, 2, 4, 8, 6} {
		if !sh.Insert(k, k*10) {
			t.Fatalf("insert failed for key=%d", k)
		}
	}
	if !sh.Remove(3) {
		t.Fatalf("remove failed for key=3")
	}

	entries := sh.Range(2, 6)
	gotKeys := make([]int, 0, len(entries))
	for _, e := range entries {
		gotKeys = append(gotKeys, e.Key)
	}
	wantKeys := []int{2, 4, 5, 6}
	if !reflect.DeepEqual(gotKeys, wantKeys) {
		t.Fatalf("unexpected range keys: got=%v want=%v", gotKeys, wantKeys)
	}

	ceil, ok := sh.Ceil(3)
	if !ok || ceil.Key != 4 {
		t.Fatalf("unexpected ceil(3): ok=%v key=%d", ok, ceil.Key)
	}
	succ, ok := sh.Succ(5)
	if !ok || succ.Key != 6 {
		t.Fatalf("unexpected succ(5): ok=%v key=%d", ok, succ.Key)
	}
	floor, ok := sh.Floor(3)
	if !ok || floor.Key != 2 {
		t.Fatalf("unexpected floor(3): ok=%v key=%d", ok, floor.Key)
	}
	pred, ok := sh.Pred(5)
	if !ok || pred.Key != 4 {
		t.Fatalf("unexpected pred(5): ok=%v key=%d", ok, pred.Key)
	}
}

func TestSkipHashConcurrentSanity(t *testing.T) {
	sh := NewSkipHash[int, int](WithRandSource(rand.NewSource(4)))
	const (
		workers  = 8
		opsPerG  = 5000
		universe = 2048
	)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		seed := int64(100 + w)
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for i := 0; i < opsPerG; i++ {
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
		}()
	}
	wg.Wait()

	if sh.Len() < 0 {
		t.Fatalf("len should never be negative")
	}
}

func TestSkipHashRangeCount(t *testing.T) {
	sh := NewSkipHash[int, int](WithRandSource(rand.NewSource(time.Now().UnixNano())))
	for i := 0; i < 100; i++ {
		sh.Store(i, i)
	}
	for i := 10; i < 20; i++ {
		sh.Remove(i)
	}

	got := sh.RangeCount(0, 99)
	if got != 90 {
		t.Fatalf("unexpected range count: got=%d want=90", got)
	}
}
