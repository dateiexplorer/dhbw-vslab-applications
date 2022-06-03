package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"github.com/jtaczanowski/go-graphite-client"
)

const (
	Broker             = "10.50.15.52"
	Group              = "vlvs_inf19b-5703004-tankerkoenig"
	GraphiteHost       = "10.50.15.52"
	GraphitePort       = 2003
	GraphiteMetricsPfx = "vlvs_inf19b.5703004.tankerkoenig"
	GraphiteProtocol   = "tcp"

	TimestampFormat = "2006-01-02T15:04:05.000-07:00"
)

var (
	topic          = "tankerkoenig"
	graphiteClient *graphite.Client
)

type TankerkoenigAggregator struct {
	entries []TankerkoenigEntry
}

type TankerkoenigEntry struct {
	date     time.Time
	station  uuid.UUID
	postCode string
	pDiesel  float64
	pE5      float64
	pE10     float64
}

func (t *TankerkoenigEntry) UnmarshalJSON(data []byte) error {
	var tmp struct {
		Date     string  `json:"date"`
		Station  string  `json:"station"`
		PostCode string  `json:"postCode"`
		PDiesel  float64 `json:"pDiesel"`
		PE5      float64 `json:"pE5"`
		PE10     float64 `json:"pE10"`
	}

	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}
	t.station = uuid.MustParse(tmp.Station)
	t.postCode = tmp.PostCode
	t.pDiesel = tmp.PDiesel
	t.pE5 = tmp.PE5
	t.pE10 = tmp.PE10

	date, err := time.Parse(TimestampFormat, tmp.Date)
	if err != nil {
		return err
	}
	t.date = date
	return nil
}

func (t TankerkoenigEntry) String() string {
	return fmt.Sprintf("{ date: %v, station: %v, postCode: %v, pDiesel: %v, pE5: %v, pE10: %v }",
		t.date, t.station.String(), t.postCode, t.pDiesel, t.pE5, t.pE10)
}

func sendTankerkoenigDataToGraphite(msg *kafka.Message) {
	var tankerkoenigEntry TankerkoenigEntry
	if err := json.Unmarshal(msg.Value, &tankerkoenigEntry); err != nil {
		log.Printf("cannot parse message '%v' in  tankerkoenig entry: %v\n",
			string(msg.Value), err)
	} else {
		fmt.Println(tankerkoenigEntry)
	}

	// // Use the lowercase variant of the city name and replace spaces with
	// // '-'. This is the case e.g. for 'Bad Mergentheim'.
	// city := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(w.City), " ", "-"))
	// var metrics = map[string]float64{
	// 	fmt.Sprintf("%v.tempCurrent", city): w.TempCurrent,
	// 	fmt.Sprintf("%v.tempMin", city):     w.TempMin,
	// 	fmt.Sprintf("%v.tempMax", city):     w.TempMax,
	// }

	// // Send data to graphite.
	// timestamp := t.date.Unix()
	// if err := graphiteClient.SendDataWithTimeStamp(metrics, timestamp); err != nil {
	// 	fmt.Fprintf(os.Stderr, "error while sending data to graphite: %v", err)
	// } else {
	// 	log.Printf("Send data to graphite: %v with timestamp %v (local: %v)\n",
	// 		metrics, timestamp, time.Unix(timestamp, 0).Local())
	// }
}

func consumeEntriesAtPartition(partition int32) chan<- interface{} {
	stop := make(chan interface{}, 1)
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": Broker,
		// A 'group.id' is neccessary, set it to a default value.
		"group.id": Group,
	})

	if err != nil {
		log.Fatalf("failed to create consumer: %v\n", err)
	}

	paritions := []kafka.TopicPartition{{
		Topic:     &topic,
		Partition: partition,
		Offset:    0,
		Metadata:  nil,
		Error:     nil,
	}}
	err = c.Assign(paritions)
	log.Println("Consumer created! Waiting for events...")

	go func() {
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

				// TODO: Aggregate data for graphite
			}
		}

		c.Close()
	}()

	return stop
}

func main() {
	graphiteClient = graphite.NewClient(GraphiteHost, GraphitePort,
		GraphiteMetricsPfx, GraphiteProtocol)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	stopChannels := make([]chan<- interface{}, 10)
	for i := 0; i < 10; i++ {
		stopChannels[i] = consumeEntriesAtPartition(int32(i))
	}

	// Stop all aggregators
	<-stop
	for _, stopChannel := range stopChannels {
		stopChannel <- struct{}{}
	}
}
