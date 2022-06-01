# dhbw-vslab-applications

A collection of applications that make use of messaging systems for the
distributed systems lab.

This application was written in Go an makes use of the following go project:
- https://github.com/confluentinc/confluent-kafka-go (as Kafka-Library)
- https://github.com/eclipse/paho.mqtt.golang (as MQTT library)
- https://github.com/google/uuid (to generate unique IDs)
- https://golang.org/x/net/websocket (for communication with frontend)

## Disclaimer

This repository is only for educational purposes and should not be used in any
productive scenarios.

It is not under active development.

To run the applications successfully, you'll need a properly set up VPN
connection to the DHBW Mosbach VPN-Server.

The protocols for some questions about basic understanding of the technologies
are in the `docs` folder.

## Quickstart

Download the repository, e.g.:

```sh
git clone https://github.com/dateiexplorer/dhbw-vslab-applications.git
cd dhbw-vslab-applications
```

To run an example, you'll need a Go and the libraries properly installed on
your system. If your Go tool chain is setup properly you can run the following
command to download automatically all needed dependencies:

```sh
go mod download
```

**Note:** If you build the applications with the `go build` command this
step isn't necessary, because the `go build` command will download all needed
dependencies automatically.

To run an example, you can execute the following command from the root
directory:

```sh
go run ./cmd/<application_name>
```

where `<application_name>` is the name of the application you want to start.

For more information about the specific examples, look in their README files.

## Build applications

You can also build the examples as binary executables using the following
command:

```sh
go build -o build/ ./...
```

This builds all applications and save them in a `build/` folder in the projects
root. Afterwards you can execute an example by running:

```sh
./build/<application_name>
```
