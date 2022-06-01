package wrapper

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/dateiexplorer/dhbw-kafkaapplications/internal/data"
)

type WeatherDataHandler func(*data.WeatherData)

func onMessageReceived(msg *kafka.Message, handler WeatherDataHandler) {
	var weatherData data.WeatherData
	if err := json.Unmarshal(msg.Value, &weatherData); err != nil {
		log.Printf("cannot parse message '%v' in weather data: %v\n",
			string(msg.Value), err)
	} else {
		handler(&weatherData)
	}
}

func RunKafkaWeatherDataConsumer(broker, topic string, stop <-chan os.Signal, handler WeatherDataHandler) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		// A 'group.id' is neccessary, set it to a default value.
		"group.id": "test-consumer-group",
	})
	defer c.Close()

	if err != nil {
		log.Fatalf("failed to create consumer: %v\n", err)
	}

	err = c.Subscribe(topic, nil)
	log.Println("Consumer created! Waiting for events...")

	run := true
	for run {
		select {
		case <-stop:
			run = false
		default:
			// Poll events, timeout 100 milliseconds
			// Waiting actively for new messages. This ensures that no outdated
			// messages are read.
			// See https://github.com/confluentinc/confluent-kafka-go for more
			// details.
			msg, err := c.ReadMessage(100)
			if err != nil {
				// Ignore the timout error.
				if err.(kafka.Error).Code() == kafka.ErrTimedOut {
					continue
				}
				// The client will automatically try to recover from all errors.
				fmt.Fprintf(os.Stderr, "Consumer error: %v (%v)\n", err, msg)
				continue
			}

			onMessageReceived(msg, handler)
		}
	}
}
