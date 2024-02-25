package kafka

import (
	"github.com/segmentio/kafka-go"
)

type Options struct {
	Brokers     []string
	Topic       string
	Partition   int
	Replication int
	Balancer    kafka.Balancer //распределяет нагрузку между брокерами
}

func WithPartitions(partition int) func(k *Kafka) {
	return func(k *Kafka) {
		k.Config.Partition = partition
	}
}

func WithReplications(replication int) func(k *Kafka) {
	return func(k *Kafka) {
		k.Config.Replication = replication
	}
}

func WithBalancer(balancer kafka.Balancer) func(k *Kafka) {
	return func(k *Kafka) {
		k.Config.Balancer = balancer
	}
}