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
	// Kafka configuration
	Broker = "10.50.15.52"
	Group  = "vlvs_inf19b-5703004-tankerkoenig"

	// Graphite configuration
	GraphiteHost       = "10.50.15.52"
	GraphitePort       = 2003
	GraphiteMetricsPfx = "vlvs_inf19b.5703004.tankerkoenig"
	GraphiteProtocol   = "tcp"

	// Application specific configuration
	TimestampFormat     = "2006-01-02T15:04:05.000-07:00"
	AggregationInterval = 1 * time.Hour
	PostCodes           = 10
)

var (
	topic          = "tankerkoenig"
	graphiteClient *graphite.Client
)

// A TankerkoenigAggregator aggregates the data from TankerkoenigEntries within
// a time period interval.
type TankerkoenigAggregator struct {
	entries  []*TankerkoenigEntry
	interval time.Duration
}

func NewTankerkoenigAggregator(interval time.Duration) *TankerkoenigAggregator {
	return &TankerkoenigAggregator{
		entries:  []*TankerkoenigEntry{},
		interval: interval,
	}
}

func (t *TankerkoenigAggregator) add(entry *TankerkoenigEntry) {
	t.entries = append(t.entries, entry)
}

func (t *TankerkoenigAggregator) empty() bool {
	return len(t.entries) == 0
}

func (t *TankerkoenigAggregator) reachedInterval() bool {
	// If the aggregation interval is reached, aggregate...
	return t.entries[len(t.entries)-1].date.Sub(t.entries[0].date) > t.interval
}

// aggregate aggregates TankerkoenigEntries and returns a TankerkoenigAggregationEntry.
// After the aggregation the TankerkoenigAggregator will be empty.
func (t *TankerkoenigAggregator) aggregate(postCode int32) *TankerkoenigAggregationEntry {
	pDiesel := 0.0
	pE5 := 0.0
	pE10 := 0.0
	for _, entry := range t.entries {
		pDiesel += entry.pDiesel
		pE5 += entry.pE5
		pE10 += entry.pE10
	}
	length := float64(len(t.entries))

	tankerkoenigAggregationEntry := &TankerkoenigAggregationEntry{
		timestamp: t.entries[0].date,
		postCode:  postCode,
		pDiesel:   pDiesel / length,
		pE5:       pE5 / length,
		pE10:      pE10 / length,
	}

	// Delete all entries after aggregation
	t.entries = []*TankerkoenigEntry{}
	return tankerkoenigAggregationEntry
}

// A TankerkoenigAggregationEntry represents an entry produced by the
// TankerkoenigAggregator and can be stored in a database, e.g. Graphite.
type TankerkoenigAggregationEntry struct {
	timestamp time.Time
	postCode  int32
	pDiesel   float64
	pE5       float64
	pE10      float64
}

func (t TankerkoenigAggregationEntry) String() string {
	return fmt.Sprintf("{ timestamp: %v, postCode: %v, pDiesel: %v, pE5: %v, pE10: %v }",
		t.timestamp, t.postCode, t.pDiesel, t.pE5, t.pE10)
}

// A TankerkoenigEntry represents an entry from the Kafka tankerkoenig topic.
type TankerkoenigEntry struct {
	date     time.Time
	station  uuid.UUID
	postCode string
	pDiesel  float64
	pE5      float64
	pE10     float64
}

func NewTankerkoenigEntryFromKafkaMessage(msg *kafka.Message) (*TankerkoenigEntry, error) {
	var tankerkoenigEntry TankerkoenigEntry
	if err := json.Unmarshal(msg.Value, &tankerkoenigEntry); err != nil {
		log.Printf("cannot parse message '%v' in  tankerkoenig entry: %v\n",
			string(msg.Value), err)
		return nil, err
	}

	return &tankerkoenigEntry, nil
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

// sendTankerkoenigDataToGraphite send a TankerkoenigAggregationEntry e to a
// Graphite database. It uses the timestamp of the
// TankerkoenigAggregationEntry.
func sendTankerkoenigDataToGraphite(e *TankerkoenigAggregationEntry) {
	var metrics = map[string]float64{
		fmt.Sprintf("%v.pDiesel", e.postCode): e.pDiesel,
		fmt.Sprintf("%v.pE5", e.postCode):     e.pE5,
		fmt.Sprintf("%v.pE10", e.postCode):    e.pE10,
	}

	// Send data to graphite.
	timestamp := e.timestamp.Unix()
	if err := graphiteClient.SendDataWithTimeStamp(metrics, timestamp); err != nil {
		fmt.Fprintf(os.Stderr, "error while sending data to graphite: %v", err)
	} else {
		log.Printf("Send data to graphite: %v with timestamp %v (local: %v)\n",
			metrics, timestamp, time.Unix(timestamp, 0).Local())
	}
}

func consumeEntriesAtPartition(partition int32) chan<- interface{} {
	stop := make(chan interface{}, 1)
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": Broker,
		// A 'group.id' is neccessary, set it to the Group value.
		"group.id": Group,
	})

	if err != nil {
		log.Fatalf("failed to create consumer: %v\n", err)
	}

	// Choose a partition, read from the beginning.
	paritions := []kafka.TopicPartition{{
		Topic:     &topic,
		Partition: partition,
		Offset:    0,
	}}

	// Assign the partition to the consumer.
	err = c.Assign(paritions)
	log.Println("Consumer created! Waiting for events...")

	// Create an aggregator for each consumer.
	aggregator := NewTankerkoenigAggregator(AggregationInterval)
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

				tankerkoenigEntry, err := NewTankerkoenigEntryFromKafkaMessage(msg)
				if err != nil {
					continue
				}

				// If no entries in the slice yet, add this entry.
				if aggregator.empty() {
					aggregator.add(tankerkoenigEntry)
					continue
				}

				// Otherwise append to the current aggreation list
				aggregator.add(tankerkoenigEntry)
				if aggregator.reachedInterval() {
					tankerkoenigAggregationEntry := aggregator.aggregate(partition)
					sendTankerkoenigDataToGraphite(tankerkoenigAggregationEntry)
				}
			}
		}

		// Send rest of data even if aggregation interval not reached
		tankerkoenigAggregationEntry := aggregator.aggregate(partition)
		sendTankerkoenigDataToGraphite(tankerkoenigAggregationEntry)

		c.Close()
	}()

	return stop
}

func main() {
	graphiteClient = graphite.NewClient(GraphiteHost, GraphitePort,
		GraphiteMetricsPfx, GraphiteProtocol)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	stopChannels := make([]chan<- interface{}, PostCodes)

	// Run a consumer for each parition, where the total number of
	// PostCode ranges (0-9) equals the number of partitions in this case.
	for i := 0; i < PostCodes; i++ {
		stopChannels[i] = consumeEntriesAtPartition(int32(i))
	}

	// Stop all aggregators
	<-stop
	for _, stopChannel := range stopChannels {
		stopChannel <- struct{}{}
	}
}
