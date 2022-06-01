# mqtt_weather

This application prints periodically weather data for a specific location.

## Usage 

To build this application, execute the following command from the projects
root directory:

```sh
go build -o build/ ./cmd/mqtt_weather
```

Make sure that you're connected to the DHBW Mosbach VPN-Server with the 'Lehre'
profile. If you using Linux you can use `openconnect` by running:

```
openconnect https://vpn.mosbach.dhbw.de/Lehre
```

and login with your user credentials.
After that you can run the binary with the following command:

```sh
./build/mqtt_weather -l <location>
```

where `<location>` defines the location for which the weather data should be
get.

## Example

Run the  weather application with the following location:
```sh
mqtt_weather -l mosbach
```

You'll get periodically the weather data for Mosbach pretty printed on the
terminal, e.g.:

```
Weather data for 'Mosbach' at 2022-05-06 12:10:10.124 +0000 +0000:
  Comment: Publ.Id 8801
  Current Temp (in °C): 18.529999
  Min Temp (in °C): 18.100006
  Max Temp (in °C): 20.369995
```
