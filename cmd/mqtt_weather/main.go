package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/dateiexplorer/dhbw-vslab-applications/internal/data"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	Host            = "10.50.12.150"
	Port            = 1883
	TimestampFormat = "2006-01-02T15:04:05.000-07:00"
)

var (
	topic string
)

// f handles an incoming message over the MQTT protocol
var f mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	var w data.WeatherData
	if err := json.Unmarshal(msg.Payload(), &w); err != nil {
		fmt.Fprintf(os.Stderr, "Error while receiving data: %v", err)
	} else {
		fmt.Println(w)
	}
}

// init initializes all neccessary global variables, e.g. from the command line
// interface.
func init() {
	var location string
	flag.StringVar(&location, "l", "", "Location for weather data.")
	flag.Parse()

	if location == "" {
		fmt.Fprintln(os.Stderr, "ERROR: you must specify a location.")
		flag.Usage()
		os.Exit(1)
	}
	// Generate a topic from the location
	topic = fmt.Sprintf("/weather/%s", location)
}

func main() {
	// Set client options
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%v:%v", Host, Port))
	opts.SetDefaultPublishHandler(f)

	c := mqtt.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}

	// Subscribe topic
	if token := c.Subscribe(topic, 0, nil); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}

	// Wait for kill and clean up
	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	<-done

	if token := c.Unsubscribe(topic); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}

	c.Disconnect(250)
}
