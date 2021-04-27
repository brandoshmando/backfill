package backfill

import (
	"sync/atomic"
)

type offsetCounter struct {
	latest   int64
	position int64
}

func (oc *offsetCounter) advance() {
	atomic.AddInt64(&oc.position, 1)
}

func (oc *offsetCounter) exhausted() bool {
	return oc.latest <= oc.position
}

func (oc *offsetCounter) delta() int64 {
	d := oc.latest - oc.position

	// never return negative
	if d >= 0 {
		return d
	}

	return 0
}

func newOffsetCounter(latest int64) *offsetCounter {
	return &offsetCounter{latest, 0}
}

type Meter map[int]*offsetCounter

func (m Meter) withPartition(partition int, latest int64) Meter {
	m[partition] = newOffsetCounter(latest)

	return m
}

func (m Meter) increment(partition int) int {
	counter, ok := m[partition]
	if !ok {
		// call unsuccessful
		// meter cannot be incremented because it does not exist
		return 0
	}

	if counter.exhausted() {
		// call successful
		// counter for given partition cannot be incremented further
		return -2
	}

	// increment partition offset position
	counter.advance()

	// call successful
	// meter incremented for given partition
	return -1
}

func (m Meter) exhausted() bool {
	var remainder int64
	for _, v := range m {
		remainder += v.delta()
	}

	return remainder == 0
}
