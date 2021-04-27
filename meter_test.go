package backfill

import "testing"

func TestOffsetCounter_advance(t *testing.T) {
	in := offsetCounter{1234, 0}
	for i := 0; i < 3; i++ {
		if in.position != int64(i) {
			t.Errorf("unexpected position: got %v, want %v", in.position, i)
		}

		in.advance()
	}
}

func TestOffsetCounter_exhausted(t *testing.T) {
	tests := []struct {
		scenario string
		in       offsetCounter
		expected bool
	}{
		{
			scenario: "not exhausted",
			in:       offsetCounter{1234, 0},
			expected: false,
		},
		{
			scenario: "exhausted eq",
			in:       offsetCounter{1234, 1234},
			expected: true,
		},
		{
			scenario: "exhausted gt",
			in:       offsetCounter{1234, 1235},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			out := tt.in.exhausted()
			if tt.expected != out {
				t.Errorf("unexpected value returned from exhausted call: got %v, want %v", out, tt.expected)
			}
		})
	}
}

func TestOffsetCounter_delta(t *testing.T) {
	tests := []struct {
		scenario string
		in       offsetCounter
		expected int64
	}{
		{
			scenario: "positive delta returns result",
			in:       offsetCounter{1234, 1000},
			expected: 234,
		},
		{
			scenario: "zero delta returns result",
			in:       offsetCounter{1234, 1234},
			expected: 0,
		},
		{
			scenario: "negative delta returns zero",
			in:       offsetCounter{1234, 1235},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			out := tt.in.delta()
			if tt.expected != out {
				t.Errorf("unexpected value returned from delta call: got %v, want %v", out, tt.expected)
			}
		})
	}
}

func TestOffsetCounter_newOffsetCounter(t *testing.T) {
	in := newOffsetCounter(1234)
	if in.latest != 1234 {
		t.Errorf("unexpected value instantiated for latest: got %v, want %v", in.latest, 1234)
	}

	if in.position != 0 {
		t.Errorf("unexpected value instantiated for position: got %v, want 0", in.position)
	}
}

func TestMeter_withPartition(t *testing.T) {
	in := map[int]*offsetCounter{
		0: &offsetCounter{1234, 0},
		1: &offsetCounter{5678, 0},
	}

	meter := make(Meter)
	for k, v := range in {
		meter.withPartition(k, v.latest)
	}

	for k, v := range in {
		out, ok := meter[k]
		if !ok {
			t.Errorf("partition unexpectedly missing from meter: want %v", k)
		}

		if out.latest != v.latest {
			t.Errorf("unexpected out value returned for partition %v: got %v, want %v", k, out, v)
		}

		if out.position != 0 {
			t.Errorf("position unexpectedly non-zero for patition %v: got %v", k, out.position)
		}
	}
}

func TestMeter_increment(t *testing.T) {
	partition0 := 0
	partition1 := 1
	meter := Meter{
		partition0: &offsetCounter{1234, 0},
		partition1: &offsetCounter{1234, 1234},
	}

	tests := []struct {
		scenario  string
		partition int
		expected  struct {
			out      int
			position int64
		}
	}{
		{
			scenario:  "non-exhuaseted meter advances offset position and returns -1",
			partition: partition0,
			expected: struct {
				out      int
				position int64
			}{
				out:      -1,
				position: 1,
			},
		},
		{
			scenario:  "exhuaseted meter does not advance offset position and returns -2",
			partition: partition1,
			expected: struct {
				out      int
				position int64
			}{
				out:      -2,
				position: 1234,
			},
		},
		{
			scenario:  "non-existent meter returns 0",
			partition: 3,
			expected: struct {
				out      int
				position int64
			}{
				out: 0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			out := meter.increment(tt.partition)
			if tt.expected.out != out {
				t.Errorf("unexpected value returned from increment on partition %v: got %v, want %v", tt.partition, out, tt.expected.out)
			}

			if tt.expected.out == 0 {
				return
			}

			counter, _ := meter[tt.partition]
			if tt.expected.position != counter.position {
				t.Errorf("unexpected position on partition %v after increment call: got %v, want %v", tt.partition, counter.position, tt.expected.position)
			}
		})
	}
}

func TestMeter_exhausted(t *testing.T) {
	tests := []struct {
		scenario string
		in       Meter
		expected bool
	}{
		{
			scenario: "not exhausted - simple",
			in: Meter{
				0: &offsetCounter{1234, 1234},
				1: &offsetCounter{5678, 5000},
			},
			expected: false,
		},
		{
			scenario: "exhausted - zero",
			in: Meter{
				0: &offsetCounter{1234, 1234},
				1: &offsetCounter{5678, 5678},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.scenario, func(t *testing.T) {
			out := tt.in.exhausted()
			if out != tt.expected {
				t.Errorf("unexpected value returned from exhausted call: got %v, want %v", out, tt.expected)
			}
		})
	}
}
