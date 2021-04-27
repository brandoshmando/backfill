package backfill

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	kafka_go "github.com/segmentio/kafka-go"
)

type Message = kafka_go.Message

type HandlerFunc func(m Message)

func DefaultHandlerFunc(_ Message) {}

type Handler struct {
	backfill HandlerFunc
	process  HandlerFunc
	config   Config
	cluster  client
	filled   chan struct{}
	stop     chan struct{}
	errors   chan error
}

func (h *Handler) Filled() chan struct{} {
	return h.filled
}

func (h *Handler) Error() chan error {
	return h.errors
}

func (h *Handler) WithFiller(f HandlerFunc) *Handler {
	safe := DefaultHandlerFunc
	if f != nil {
		safe = f
	}

	h.backfill = safe
	return h
}

func (h *Handler) WithProcessor(f HandlerFunc) *Handler {
	h.process = f
	return h
}

func initMeter(c client, topic string) (Meter, error) {
	m := make(Meter)

	partitions, err := c.fetchPartitions(topic)
	if err != nil {
		return m, err
	}

	offsets, err := c.fetchLatestOffsets(topic, partitions)
	if err != nil {
		return m, err
	}

	for p, l := range offsets {
		m.withPartition(p, l)
	}

	return m, nil
}

func handle(h *Handler, meter Meter, m Message) {
	process := func() {
		f := h.backfill
		if h.process != nil {
			f = h.process
		}

		f(m)
	}

	if meter.exhausted() {
		process()
		return
	}

	switch meter.increment(m.Partition) {
	case 0:
		panic(fmt.Sprintf("meter failed to increment for partition %d", m.Partition))
	case -2:
		process()
		return
	}

	h.backfill(m)

	if meter.exhausted() {
		close(h.filled)
	}
}

func (h *Handler) Start(ctx context.Context) error {
	// setup necessary kafka clients / connections
	// fetch last offsets and instantiate/populate meter
	// fire goroutine that starts message processing:
	// - if backfill, call backfill func and increment meter
	// - when backfill complete, close BackFilled chan
	// - if backfill complete, call process func if non-nil or call backfill func
	// allow for clean teardown

	// kafka-go setup:
	// get connection
	// get controller
	// conn.Controller addr for API requests / Writes
	// conn.Brokers for reader broker list
	addrs, err := h.cluster.addrs()
	if err != nil {
		return err
	}

	// TODO: make this more configurable
	r := kafka_go.NewReader(kafka_go.ReaderConfig{
		Brokers:  addrs,
		Topic:    h.config.Topic,
		GroupID:  h.config.GroupID,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	r.SetOffset(kafka_go.LastOffset)

	meter, err := initMeter(h.cluster, h.config.Topic)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		<-h.stop
		cancel()
	}()

	go func() {
		for {
			m, err := r.ReadMessage(ctx)
			if err != nil {
				h.errors <- err
				break
			}

			handle(h, meter, m)
		}
	}()

	return nil
}

func (h *Handler) Stop() {
	close(h.stop)
}

func namespacedGroupID(ns string) string {
	return fmt.Sprintf("_backfill.%s.%s", ns, uuid.New().String())
}

func NewHandler(c Config) (*Handler, error) {
	cluster, err := newCluster(c.Host)
	if err != nil {
		return nil, err
	}

	return &Handler{
		backfill: DefaultHandlerFunc,
		config:   c,
		cluster:  cluster,
		filled:   make(chan struct{}, 1),
		stop:     make(chan struct{}),
		errors:   make(chan error, 1),
	}, nil
}
