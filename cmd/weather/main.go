package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/dateiexplorer/dhbw-kafkaapplications/internal/data"
	"github.com/dateiexplorer/dhbw-kafkaapplications/internal/wrapper"
)

const (
	Broker = "10.50.15.52"
)

var (
	topic = "weather"
)

func printWeatherData(w *data.WeatherData) {
	fmt.Println(w)
}

func main() {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	// This wrapper blocks the main thread until a signal is received.
	wrapper.RunKafkaWeatherDataConsumer(Broker, topic, stop, printWeatherData)
}
