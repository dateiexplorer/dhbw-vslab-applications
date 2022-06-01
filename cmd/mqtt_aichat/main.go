package main

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"strings"
	"text/template"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"golang.org/x/net/websocket"
)

//go:embed web/*
var content embed.FS

const (
	MQTTHost = "10.50.12.150"
	MQTTPort = 1883

	ChatRootTopic        = "/aichat/"
	ChatClientStateTopic = "clientstate"
	ChatDefaultRoom      = "default"

	UIPort = 5556
)

var userClient *UserClient
var msgQueue chan *Message

type Message struct {
	Sender   string `json:"sender"`
	Text     string `json:"text"`
	ClientID string `json:"clientId"`
	Topic    string `json:"topic"`
}

type LoginCredentials struct {
	Name string
	Room string
}

type UserClient struct {
	internal mqtt.Client
	clientID string
	Messages []*Message
	Name     string
	Room     string
}

func (u *UserClient) JoinRoom(c *LoginCredentials) error {
	topic := fmt.Sprintf("%v%v", ChatRootTopic, c.Room)
	token := u.internal.Subscribe(topic, 0, nil)
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("cannot subscribe topic '%v': %w", c.Room, token.Error())
	}

	u.Name = c.Name
	u.Room = c.Room
	return nil
}

func publishHandler() (chan *Message, mqtt.MessageHandler) {
	// Create a message queue
	msgQueue := make(chan *Message)
	f := func(client mqtt.Client, msg mqtt.Message) {
		// Get new messages from MQTT and write them in the message queue.
		var data Message
		if err := json.Unmarshal(msg.Payload(), &data); err != nil {
			log.Printf("cannot receive data from mqtt: %v\n", err)
			return
		}
		msgQueue <- &data
	}
	return msgQueue, f
}

func websocketHandler(ws *websocket.Conn) {
	// Run message handler in separate goroutine.
	runMessageHandler(ws, msgQueue)

	for {
		var receivedData []byte

		if err := websocket.Message.Receive(ws, &receivedData); err != nil {
			log.Printf("cannot receive data from ws: %v\n", err)
			return
		}

		var data struct {
			Text string `json:"text"`
		}
		json.Unmarshal(receivedData, &data)

		// Create message from received data.
		message := &Message{
			Sender:   userClient.Name,
			Text:     data.Text,
			ClientID: userClient.clientID,
			Topic:    fmt.Sprintf("%v%v", ChatRootTopic, userClient.Room)}
		json, _ := json.Marshal(message)

		token := userClient.internal.Publish(message.Topic, byte(0), false, json)
		if token.Wait() && token.Error() != nil {
			log.Println("websocketHandler", token.Error())
		}
	}
}

func runMessageHandler(ws *websocket.Conn, msgQueue chan *Message) {
	go func() {
		for msg := range msgQueue {
			onMsgReceived(ws, msg)
		}
	}()
}

type extendedMsg struct {
	*Message
	Me        string `json:"me"`
	Timestamp int64  `json:"timeStamp"`
}

func onMsgReceived(ws *websocket.Conn, msg *Message) {
	extendedMsg := extendedMsg{
		msg, userClient.clientID, time.Now().Local().UnixMilli(),
	}

	bytes, _ := json.Marshal(extendedMsg)
	log.Println("onMsgReceived", string(bytes))

	if err := websocket.Message.Send(ws, string(bytes)); err != nil {
		log.Printf("cannot send data to ws: %v\n", err)
		// This is a workaround for simple usage.
		// if the message couldn't be send to the websocket, close this
		// socket and send the message back into the queue.
		// WARNING: If no websocket is available the queue will flooeded
		// with the hanging message and performs an endless loop until
		// a new websocket is connected which can handle the message.
		ws.Close()
		msgQueue <- msg
	}
}

func generateRandomClientID() string {
	return uuid.NewString()
}

func connectToMQTT(defaultHandler mqtt.MessageHandler) *UserClient {
	// Set client options
	opts := mqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("tcp://%v:%v", MQTTHost, MQTTPort)).
		SetClientID(generateRandomClientID()).
		SetDefaultPublishHandler(defaultHandler)
	// Set 'Last Will' message
	opts.SetWill(
		fmt.Sprintf("%v%v", ChatRootTopic, ChatClientStateTopic),
		fmt.Sprintf("Chat Client %v stopped", opts.ClientID),
		byte(0), false)

	// Create client
	c := mqtt.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		log.Println("connectToMQTT", token.Error())
		os.Exit(1)
	}
	// Send welcome message
	c.Publish(
		fmt.Sprintf("%v%v", ChatRootTopic, ChatClientStateTopic),
		byte(0), false,
		fmt.Sprintf("Chat Client %v started", opts.ClientID))

	return &UserClient{c, opts.ClientID, make([]*Message, 0), "", ""}
}

func main() {
	mq, publishHandler := publishHandler()
	msgQueue = mq

	userClient = connectToMQTT(publishHandler)

	// Set up web assets
	assets, _ := fs.Sub(content, "web/static")
	http.Handle("/assets/", http.StripPrefix("/assets/", http.FileServer(http.FS(assets))))

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		// Get URL parameter
		name := strings.TrimSpace(query.Get("name"))
		room := strings.TrimSpace(query.Get("room"))

		// If either of the parameters are set to null, show login page.
		if name == "" || room == "" {
			t := template.Must(template.ParseFS(content, "web/templates/login.html"))
			t.Execute(w, &LoginCredentials{"", ChatDefaultRoom})
			return
		}
		// URL parameters are set
		loginCredentials := &LoginCredentials{name, room}
		if err := userClient.JoinRoom(loginCredentials); err != nil {
			log.Printf("cannot join room: %v\n", err)
			t := template.Must(template.ParseFS(content, "web/templates/login.html"))
			t.Execute(w, &LoginCredentials{"", ChatDefaultRoom})
			return
		} else {
			log.Printf("client '%v' with name '%v' joined room '%v'\n",
				userClient.clientID, userClient.Name, userClient.Room)
		}

		// Show main application
		t := template.Must(template.ParseFS(content, "web/templates/index.html"))
		t.Execute(w, loginCredentials)
	})

	http.Handle("/ws", websocket.Handler(websocketHandler))

	// Start web server
	log.Fatalln(http.ListenAndServe(fmt.Sprintf(":%v", UIPort), nil))
}
