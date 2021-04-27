package backfill

import (
	"errors"
	"testing"

	kafka_go "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/mock"
)

func TestHandler_initMeter(t *testing.T) {
	tests := []struct {
		scenario string
		err      bool
		mock     func() (client, func(*testing.T))
		expected Meter
	}{
		{
			scenario: "meter is hydrated with partitions and offsets returned from client calls",
			err:      false,
			mock: func() (client, func(*testing.T)) {
				partitions := []kafka_go.Partition{
					{ID: 0},
					{ID: 1},
				}
				offsets := map[int]int64{
					0: 1234,
					1: 5678,
				}

				var m mockClient
				m.
					On("fetchPartitions", "topic").
					Return(partitions, nil).
					Once().
					On("fetchLatestOffsets", "topic", partitions).
					Return(offsets, nil).
					Once()

				return &m, func(_ *testing.T) {}
			},
			expected: Meter{
				0: &offsetCounter{1234, 0},
				1: &offsetCounter{5678, 0},
			},
		},
		{
			scenario: "error is returned from fetchPartitions, meter remains empty",
			err:      true,
			mock: func() (client, func(*testing.T)) {
				var m mockClient
				m.
					On("fetchPartitions", "topic").
					Return(
						make([]kafka_go.Partition, 0),
						errors.New("test error"),
					).
					Once()

				assertions := func(t *testing.T) {
					m.AssertNotCalled(t, "fetchLatestOffsets")
				}

				return &m, assertions
			},
			expected: make(Meter),
		},
		{
			scenario: "error is returned from fetchPartitions, meter remains empty",
			err:      true,
			mock: func() (client, func(*testing.T)) {
				partitions := []kafka_go.Partition{
					{ID: 0},
					{ID: 1},
				}

				var m mockClient
				m.
					On("fetchPartitions", "topic").
					Return(partitions, nil).
					Once().
					On("fetchLatestOffsets", "topic", partitions).
					Return(
						make(map[int]int64),
						errors.New("test error"),
					)

				return &m, func(_ *testing.T) {}
			},
			expected: make(Meter),
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			client, assertions := tt.mock()

			out, err := initMeter(client, "topic")
			if tt.err != (err != nil) {
				t.Errorf("unexpected error value returned from initMeter: got %v", err)
			}

			mock.AssertExpectationsForObjects(t, client)
			assertions(t)
			for k, v := range tt.expected {
				o, ok := out[k]
				if !ok {
					t.Errorf("partition %v unexpectedly missing after initMeter", k)
				}

				if v.latest != o.latest {
					t.Errorf("unexpected latest value set for partition %v: got %v, want %v", k, o.latest, v.latest)
				}

				if v.position != o.position {
					t.Errorf("unexpected position value set for partition %v: got %v, want %v", k, o.position, v.position)
				}
			}
		})
	}
}
