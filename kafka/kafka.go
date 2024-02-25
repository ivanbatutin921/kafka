package kafka

import (
	//"github.com/grpc-ecosystem/grpc-gateway/protoc-gen-swagger/options"
	"fmt"
	"net"
	"strconv"

	"github.com/segmentio/kafka-go"
)

type Kafka struct {
	Config Options
	conn   *kafka.Conn
	broker *kafka.Broker
}

func New(brokers []string, topic string, options ...func(*Kafka)) *Kafka {
	var err error

	//Kafka
	k := &Kafka{
		Config: Options{
			Brokers:     brokers,
			Topic:       topic,
			Partition:   3,
			Replication: 1,
		},
	}

	//итерация по фрагменту параметров и вызывает каждую функцию в фрагменте с аргументом k.
	for _, option := range options {
		option(k)
	}

	//conn
	k.conn, err = kafka.Dial("tcp", k.Config.Brokers[0])
	if err != nil {
		panic(err.Error())
	}

	//broker
	*k.broker, err = k.conn.Controller()
	if err != nil {
		panic(err.Error())
	}

	//установление соединения с брокером
	k.conn, err = kafka.Dial("tcp", net.JoinHostPort(k.broker.Host, strconv.Itoa(k.broker.Port)))
	if err != nil {
		panic(err.Error())
	}

	//создание топика
	k.createTopicConfig()

	fmt.Print("Topic created")
	return k
}

func (k *Kafka) createTopicConfig() kafka.TopicConfig {
	return kafka.TopicConfig{
		Topic:             k.Config.Topic,
		NumPartitions:     k.Config.Partition,
		ReplicationFactor: k.Config.Replication,
	}
}

