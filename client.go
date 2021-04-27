package backfill

import (
	"context"
	"fmt"

	kafka_go "github.com/segmentio/kafka-go"
)

type client interface {
	addrs() ([]string, error)
	fetchPartitions(topic string) ([]kafka_go.Partition, error)
	fetchLatestOffsets(topic string, partitions []kafka_go.Partition) (map[int]int64, error)
}

type cluster struct {
	conn *kafka_go.Conn
}

func (c *cluster) addrs() ([]string, error) {
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

func (c *cluster) fetchPartitions(topic string) ([]kafka_go.Partition, error) {
	b, err := c.conn.Controller()
	if err != nil {
		return []kafka_go.Partition{}, err
	}

	return kafka_go.LookupPartitions(context.TODO(), "tcp", addrFromBroker(b), topic)
}

func addrFromBroker(b kafka_go.Broker) string {
	return fmt.Sprintf("%s:%d", b.Host, b.Port)
}

func (c *cluster) fetchLatestOffsets(topic string, partitions []kafka_go.Partition) (map[int]int64, error) {
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
		req.Topics[topic] = append(req.Topics[topic], r)
	}

	latest := make(map[int]int64)
	res, err := client.ListOffsets(context.TODO(), &req)
	if err != nil {
		return latest, err
	}

	offsets, ok := res.Topics[topic]
	if !ok {
		return latest, fmt.Errorf("could not fetch latest offsets: topic %s does not exist", topic)
	}

	for _, r := range offsets {
		latest[r.Partition] = r.LastOffset
	}

	return latest, nil
}

func newCluster(host string) (*cluster, error) {
	conn, err := kafka_go.Dial("tcp", host)
	if err != nil {
		return nil, err
	}

	return &cluster{conn}, nil
}
