package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

// func NewProducer() *Producer {
// 	return &Producer{
// 		writer: kafka.NewWriter(kafka.WriterConfig{
// 			Brokers: []string{"localhost:9092"},
// 			Topic:   "test",
// 		}),
// 	}
// }

func (p *Producer) Produce(message string) error {
	return p.writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte("key"),
		Value: []byte(message),
	})
}