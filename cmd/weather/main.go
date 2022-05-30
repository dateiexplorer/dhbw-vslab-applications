package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	Broker          = "10.50.15.52"
	TimestampFormat = "2006-01-02T03:04:05.000-07:00"
)

var (
	topic = "weather"
)

type WeatherData struct {
	tempCurrent float64
	tempMax     float64
	tempMin     float64
	comment     string
	timeStamp   time.Time
	city        string
	cityID      int
}

// String returns a pretty printed weather data record to print on the command
// line.
func (d WeatherData) String() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("Weather data for '%v' at %v:\n", d.city, d.timeStamp))
	b.WriteString(fmt.Sprintf("  Comment: %v\n", d.comment))
	b.WriteString(fmt.Sprintf("  Current Temp (in °C): %v\n", d.tempCurrent))
	b.WriteString(fmt.Sprintf("  Min Temp (in °C): %v\n", d.tempMin))
	b.WriteString(fmt.Sprintf("  Max Temp (in °C): %v\n", d.tempMax))
	return b.String()
}

func (d *WeatherData) UnmarshalJSON(data []byte) error {
	var tmp struct {
		TempCurrent float64 `json:"tempCurrent"`
		TempMax     float64 `json:"tempMax"`
		TempMin     float64 `json:"tempMin"`
		Comment     string  `json:"comment"`
		TimeStamp   string  `json:"timeStamp"`
		City        string  `json:"city"`
		CityID      int     `json:"cityId"`
	}

	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}
	d.tempCurrent = tmp.TempCurrent
	d.tempMax = tmp.TempMax
	d.tempMin = tmp.TempMin
	d.comment = tmp.Comment
	d.city = tmp.City
	d.cityID = tmp.CityID

	timeStamp, err := time.Parse(TimestampFormat, tmp.TimeStamp)
	if err != nil {
		return err
	}
	d.timeStamp = timeStamp
	return nil
}

func printMessage(msg *kafka.Message) {
	var data WeatherData
	if err := json.Unmarshal(msg.Value, &data); err != nil {
		log.Printf("cannot parse message '%v' in weather data: %v\n",
			string(msg.Value), err)
	} else {
		fmt.Println(data)
	}
}

func main() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": Broker,
		// A 'group.id' is neccessary, set it to a default value.
		"group.id": "test-consumer-group",
	})
	defer c.Close()

	if err != nil {
		log.Fatalf("failed to create consumer: %v\n", err)
	}

	err = c.Subscribe(topic, nil)
	log.Println("Consumer created! Waiting for events...")

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)

	run := true
	for run {
		select {
		case <-done:
			run = false
		default:
			// Poll events, timeout 100 milliseconds
			// Waiting actively for new messages
			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				printMessage(e)
			case kafka.Error:
				// Errors should generally be considered informational, the
				// client will try to automatically recover.
				// But in this example we choose to terminate the application
				// if all brokers are down.
				fmt.Fprintf(os.Stderr, "Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					break
				}
			default:
				// Ignore all other messages
				continue
			}
		}
	}
}
