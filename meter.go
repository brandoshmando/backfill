package backfill

import (
	"fmt"

	"github.com/google/uuid"
)

type Meter map[int][2]int64

func (m Meter) withPartition(partition int, last int64) Meter {
	m[partition] = [2]int64{last}

	return m
}

func (m Meter) increment(partition int) int {
	current, ok := m[partition]
	if !ok {
		// call unsuccessful
		// meter cannot be incremented because it does not exist
		return 0
	}

	current[1] = current[1] + 1
	if current[0] < current[1] {
		// call successful
		// meter cannot be incremented further for given partition
		return -2
	}

	// call successful
	// meter incremented for given partition
	return -1
}

func (m Meter) exhausted() bool {
	var remainder int64
	for _, v := range m {
		remainder += (v[0] - v[1])
	}

	return remainder == 0
}

func namespacedGroupID(ns string) string {
	return fmt.Sprintf("_backfill.%s.%s", ns, uuid.New().String())
}

func newMeter(offsets map[int]int64) Meter {
	m := make(Meter)
	for p, l := range offsets {
		m.withPartition(p, l)
	}

	return m
}

func initMeter(c *cluster) (Meter, error) {
	partitions, err := c.fetchPartitions()
	if err != nil {
		return Meter{}, err
	}

	offsets, err := c.fetchLastOffsets(partitions)
	if err != nil {
		return Meter{}, err
	}

	return newMeter(offsets), nil
}
