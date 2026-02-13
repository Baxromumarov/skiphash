package skiphash

import "cmp"

type rangeCoordinator[K cmp.Ordered, V any] struct {
	counter uint64

	head *rangeOp[K, V]
	tail *rangeOp[K, V]

	byVersion map[uint64]*rangeOp[K, V]
}

type rangeOp[K cmp.Ordered, V any] struct {
	ver uint64

	deferred []*slNode[K, V]

	prev *rangeOp[K, V]
	next *rangeOp[K, V]
}

func newRangeCoordinator[K cmp.Ordered, V any]() *rangeCoordinator[K, V] {
	return &rangeCoordinator[K, V]{
		counter:   1,
		byVersion: make(map[uint64]*rangeOp[K, V]),
	}
}

func (r *rangeCoordinator[K, V]) onRangeLocked() uint64 {
	r.counter++
	op := &rangeOp[K, V]{ver: r.counter}
	if r.tail == nil {
		r.head = op
		r.tail = op
	} else {
		op.prev = r.tail
		r.tail.next = op
		r.tail = op
	}
	r.byVersion[op.ver] = op
	return op.ver
}

func (r *rangeCoordinator[K, V]) onUpdateLocked() uint64 {
	return r.counter
}

func (r *rangeCoordinator[K, V]) afterRemoveLocked(sh *SkipHash[K, V], node *slNode[K, V]) {
	if r.tail == nil || node.iTime >= r.tail.ver {
		sh.unstitchNodeLocked(node)
		return
	}
	r.tail.deferred = append(r.tail.deferred, node)
}

func (r *rangeCoordinator[K, V]) afterRangeLocked(sh *SkipHash[K, V], ver uint64) {
	op, ok := r.byVersion[ver]
	if !ok {
		return
	}
	delete(r.byVersion, ver)

	pred := op.prev
	next := op.next
	if pred == nil {
		r.head = next
	} else {
		pred.next = next
	}
	if next == nil {
		r.tail = pred
	} else {
		next.prev = pred
	}

	if pred == nil {
		for _, node := range op.deferred {
			sh.unstitchNodeLocked(node)
		}
		return
	}
	pred.deferred = append(pred.deferred, op.deferred...)
}
