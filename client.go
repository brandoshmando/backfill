package backfill

import (
	"context"
	"fmt"

	kafka_go "github.com/segmentio/kafka-go"
)

type cluster struct {
	conn    *kafka_go.Conn
	topic   string
	groupID string
}

func (c *cluster) Addrs() ([]string, error) {
	brokers, err := c.conn.Brokers()
	if err != nil {
		return make([]string, 0), err
	}

	addrs := make([]string, len(brokers))
	for i, b := range brokers {
		addrs[i] = addrFromBroker(b)
	}

	return addrs, nil
}

func (c *cluster) fetchPartitions() ([]kafka_go.Partition, error) {
	b, err := c.conn.Controller()
	if err != nil {
		return []kafka_go.Partition{}, err
	}

	return kafka_go.LookupPartitions(context.TODO(), "tcp", addrFromBroker(b), c.topic)
}

func addrFromBroker(b kafka_go.Broker) string {
	return fmt.Sprintf("%s:%d", b.Host, b.Port)
}

func (c *cluster) fetchLastOffsets(partitions []kafka_go.Partition) (map[int]int64, error) {
	b, err := c.conn.Controller()
	if err != nil {
		return make(map[int]int64), err
	}
	client := kafka_go.Client{Addr: kafka_go.TCP(addrFromBroker(b))}

	req := kafka_go.ListOffsetsRequest{
		Topics: make(map[string][]kafka_go.OffsetRequest),
	}

	for _, p := range partitions {
		r := kafka_go.LastOffsetOf(p.ID)
		req.Topics[c.topic] = append(req.Topics[c.topic], r)
	}

	last := make(map[int]int64)
	res, err := client.ListOffsets(context.TODO(), &req)
	if err != nil {
		return last, err
	}

	offsets, ok := res.Topics[c.topic]
	if !ok {
		return last, fmt.Errorf("could not fetch last offsets: topic %s does not exist", c.topic)
	}

	for _, r := range offsets {
		last[r.Partition] = r.LastOffset
	}

	return last, nil
}
