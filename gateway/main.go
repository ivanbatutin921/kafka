package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	//"github.com/segmentio/kafka-go"
	"github.com/IBM/sarama" //API для взаимодействия с Kafka.
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"

	kafka "root/kafka"
)

type Message struct {
	Id    int
	Name  string
	Value string
}

var mu sync.Mutex

func main() {
	k := kafka.New([]string{"localhost:9092"}, "test",
		kafka.WithPartitions(3),
		kafka.WithReplications(1),
		kafka.WithBalancer(&k.Hash{}),
	)

	

		// Создайте канал, который будет получать значения *sarama.Message, проиндексированные строковым ключом.
		responseChannels := make(map[string]chan *sarama.ConsumerMessage)

	//Создание продюсера Kafka
	producer, err := sarama.NewSyncProducer([]string{"kafka:9092"}, nil)
	if err != nil {
		fmt.Println("Producer creation failed:", err)
		return
	}
	defer producer.Close()

	//Создание потребителя
	consumer, err := sarama.NewConsumer([]string{"kafka:9092"}, nil)
	if err != nil {
		fmt.Println("потребитель не создался")
	}
	defer consumer.Close()

	//подписька "test2"
	partConsumer, err := consumer.ConsumePartition("test2", 0, sarama.OffsetOldest)
	if err != nil {
		fmt.Println("-подписка")
	}

	go func() {
		for {
			select {
			//чтение из кафки
			case msg, ok := <-partConsumer.Messages():
				if !ok {
					fmt.Print("не прочиталось")
					return
				}
				ResponseID := string(msg.Key)
				mu.Lock()
				ch, exists := responseChannels[ResponseID]
				if exists {
					ch <- msg
					delete(responseChannels, ResponseID)
				}
				mu.Unlock()
			}
		}
	}()

	app := fiber.New()
	app.Get("/xyu", func(c *fiber.Ctx) error {
		reguestID := uuid.New().String()

		message := Message{
			Id:    1,
			Name:  "Egor",
			Value: "lox",
		}

		bytes, err := json.Marshal(message)
		if err != nil {
			fmt.Print("не сконвертировалось")
		}

		msg := &sarama.ProducerMessage{
			Topic: "test",
			Key:   sarama.StringEncoder(reguestID),
			Value: sarama.ByteEncoder(bytes),
		}

		_, _, err = producer.SendMessage(msg)
		if err != nil {
			fmt.Print("не отправилось")
			return err
		}

		responseCh := make(chan *sarama.ConsumerMessage)
		mu.Lock()
		responseChannels[reguestID] = responseCh
		mu.Unlock()

		select {
		case msg := <-responseCh:
			c.JSON(fiber.Map{
				"message": msg.Value,
			})
		case <-time.After(5 * time.Second):
			mu.Lock()
			delete(responseChannels, reguestID)
			mu.Unlock()
			c.JSON(fiber.Map{
				"message": "timeout",
			})
		}
		return nil
	})

	app.Listen(":3000")

}

// Create a channel that will receive *sarama.Message values, indexed by a string key.
