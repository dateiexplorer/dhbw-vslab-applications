package data

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const TimestampFormat = "2006-01-02T15:04:05.000-07:00"

type WeatherData struct {
	TempCurrent float64
	TempMax     float64
	TempMin     float64
	Comment     string
	TimeStamp   time.Time
	City        string
	CityID      int
}

// String returns a pretty printed weather data record to print on the command
// line.
func (d WeatherData) String() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("Weather data for '%v' at %v:\n", d.City, d.TimeStamp))
	b.WriteString(fmt.Sprintf("  Comment: %v\n", d.Comment))
	b.WriteString(fmt.Sprintf("  Current Temp (in °C): %v\n", d.TempCurrent))
	b.WriteString(fmt.Sprintf("  Min Temp (in °C): %v\n", d.TempMin))
	b.WriteString(fmt.Sprintf("  Max Temp (in °C): %v\n", d.TempMax))
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
	d.TempCurrent = tmp.TempCurrent
	d.TempMax = tmp.TempMax
	d.TempMin = tmp.TempMin
	d.Comment = tmp.Comment
	d.City = tmp.City
	d.CityID = tmp.CityID

	timeStamp, err := time.Parse(TimestampFormat, tmp.TimeStamp)
	if err != nil {
		return err
	}
	d.TimeStamp = timeStamp
	return nil
}
