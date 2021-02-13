package backfill

import (
	"context"
	"fmt"

	kafka_go "github.com/segmentio/kafka-go"
)

type Message struct {
	kafka_go.Message
}

type HandlerFunc func(m Message)

func DefaultHandlerFunc(_ Message) {}

type Handler struct {
	backfill HandlerFunc
	process  HandlerFunc
	cluster  *cluster
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
	meter, err := initMeter(h.cluster)
	if err != nil {
		return err
	}

	addrs, err := h.cluster.Addrs()
	if err != nil {
		return err
	}

	// TODO: make this more configurable
	r := kafka_go.NewReader(kafka_go.ReaderConfig{
		Brokers:  addrs,
		Topic:    h.cluster.topic,
		GroupID:  h.cluster.groupID,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	r.SetOffset(kafka_go.LastOffset)

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

			process := func() {
				safe := h.backfill
				if h.process != nil {
					safe = h.process
				}

				safe(Message{m})
			}

			if meter.exhausted() {
				process()
				continue
			}

			switch meter.increment(m.Partition) {
			case 0:
				panic(fmt.Sprintf("meter failed to increment for partition %d", m.Partition))
			case -2:
				process()
				continue
			}

			h.backfill(Message{m})

			if meter.exhausted() {
				close(h.filled)
			}
		}
	}()

	return nil
}

func (h *Handler) Stop() {
	close(h.stop)
}

func NewHandler(c Config) (*Handler, error) {
	conn, err := kafka_go.Dial("tcp", c.Host)
	if err != nil {
		return nil, err
	}

	return &Handler{
		backfill: DefaultHandlerFunc,
		cluster:  &cluster{conn, c.Topic, namespacedGroupID(c.Namespace)},
		filled:   make(chan struct{}, 1),
		stop:     make(chan struct{}),
		errors:   make(chan error, 1),
	}, nil
}
