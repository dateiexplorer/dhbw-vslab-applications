package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/dateiexplorer/dhbw-kafkaapplications/internal/data"
	"github.com/dateiexplorer/dhbw-kafkaapplications/internal/wrapper"
	"github.com/jtaczanowski/go-graphite-client"
)

const (
	Broker             = "10.50.15.52"
	GraphiteHost       = "10.50.15.52"
	GraphitePort       = 2003
	GraphiteMetricsPfx = "vlvs_inf19b.go7f18.weather"
	GraphiteProtocol   = "tcp"
)

var (
	topic          = "weather"
	graphiteClient *graphite.Client
)

func sendWeatherDataToGraphite(w *data.WeatherData) {
	// Use the lowercase variant of the city name and replace spaces with
	// '-'. This is the case e.g. for 'Bad Mergentheim'.
	city := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(w.City), " ", "-"))
	var metrics = map[string]float64{
		fmt.Sprintf("%v.tempCurrent", city): w.TempCurrent,
		fmt.Sprintf("%v.tempMin", city):     w.TempMin,
		fmt.Sprintf("%v.tempMax", city):     w.TempMax,
	}

	// Send data to graphite.
	timestamp := w.TimeStamp.Unix()
	if err := graphiteClient.SendDataWithTimeStamp(metrics, timestamp); err != nil {
		fmt.Fprintf(os.Stderr, "error while sending data to graphite: %v", err)
	} else {
		log.Printf("Send data to graphite: %v with timestamp %v (local: %v)\n",
			metrics, timestamp, time.Unix(timestamp, 0).Local())
	}
}

func main() {
	graphiteClient = graphite.NewClient(GraphiteHost, GraphitePort,
		GraphiteMetricsPfx, GraphiteProtocol)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	// This wrapper blocks the main thread until a signal is received.
	wrapper.RunKafkaWeatherDataConsumer(Broker, topic, stop, sendWeatherDataToGraphite)
}
