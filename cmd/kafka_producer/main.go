package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
)

const (
	Broker = "10.50.15.52"
)

var (
	// Ensure that this topic is unique
	topic = "vlvs_inf19b_5703004_random"
)

type Message struct {
	Value string
}

// Generate a random message with the uuid package.
func getRandomMessage() *Message {
	value := uuid.NewString()

	return &Message{
		Value: value,
	}
}

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": Broker,
	})
	defer p.Close()

	if err != nil {
		log.Fatalf("failed to create producer: %v\n", err)
	}

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)

	run := true
	for run {
		select {
		case <-done:
			run = false
		case <-time.After(1 * time.Second):
			value, _ := json.Marshal(getRandomMessage())

			// Publish some random data...
			msg := &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic},
				Value:          value,
				Key:            value,
			}
			if err = p.Produce(msg, nil); err != nil {
				log.Printf("message cannot be produced: %v\n", err)
			} else {
				log.Printf("message produced: %v\n", string(msg.Value))
			}
		}
	}
}
