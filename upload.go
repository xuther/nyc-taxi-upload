package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

//ToSend sends
type ToSend struct {
	DropoffTime string   `json:"dropoff-time"`
	PickupTime  string   `json:"pickup-time"`
	StartBlock  string   `json:"start-block"`
	StartTract  string   `json:"start-tract"`
	StartCounty string   `json:"start-county"`
	EndBlock    string   `json:"end-block"`
	EndTract    string   `json:"end-tract"`
	EndCounty   string   `json:"end-county"`
	StartCoords Position `json:"start-coords"`
	EndCoords   Position `json:"end-coords"`
}

type Position struct {
	Latitude  float64 `json:"lat"`
	Longitude float64 `json:"lon"`
}

type Bulk struct {
	Items []ToSend `json:"items"`
}

type Config struct {
	ESAddress    string `json:"elk-address"`
	InputAddress string `json:"input-address"`
	TimeFormat   string `json:"time-format"`
	StartLat     int    `json:"start-lat"`
	StartLong    int    `json:"start-long"`
	PickupTime   int    `json:"start-time"`
	StartBlock   int    `json:"start-block"`
	StartTract   int    `json:"start-tract"`
	StartCounty  int    `json:"start-county"`
	DropoffTime  int    `json:"end-time"`
	EndLat       int    `json:"end-lat"`
	EndLong      int    `json:"end-long"`
	EndBlock     int    `json:"end-block"`
	EndTract     int    `json:"end-tract"`
	EndCounty    int    `json:"end-county"`
}

var config Config

func importConfig(path string) (c Config) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(b, &c)
	if err != nil {
		panic(err)
	}
	return
}

func translateRow(row []string) (ts ToSend, err error) {

	dt, e := time.Parse(config.TimeFormat, row[config.DropoffTime])
	if e != nil {
		err = e
		return
	}
	ts.DropoffTime = dt.Format(time.RFC3339)

	pt, e := time.Parse(config.TimeFormat, row[config.PickupTime])
	if e != nil {
		err = e
		return
	}
	ts.PickupTime = pt.Format(time.RFC3339)

	ts.StartBlock = row[config.StartBlock]
	ts.StartTract = row[config.StartTract]
	ts.StartCounty = row[config.StartCounty]
	ts.EndBlock = row[config.EndBlock]
	ts.EndTract = row[config.EndTract]
	ts.EndCounty = row[config.EndCounty]

	startLat, e := strconv.ParseFloat(row[config.StartLat], 64)
	if err != nil {
		err = e
		return
	}
	startLong, e := strconv.ParseFloat(row[config.StartLong], 64)
	if err != nil {
		err = e
		return
	}

	ts.StartCoords = Position{
		Latitude:  startLat,
		Longitude: startLong,
	}

	endLat, e := strconv.ParseFloat(row[config.EndLat], 64)
	if err != nil {
		err = e
		return
	}
	endLong, e := strconv.ParseFloat(row[config.EndLong], 64)
	if err != nil {
		err = e
		return
	}
	ts.EndCoords = Position{
		Latitude:  endLat,
		Longitude: endLong,
	}

	return
}

func main() {
	var configAddress = flag.String("configAddress", "./config.json", "Configuration File")

	end := false

	log.Printf("Starting.\n")
	var numberStarted int
	config = importConfig(*configAddress)

	f, err := os.Open(config.InputAddress)
	if err != nil {
		log.Printf("Error opening input file:\n")
		log.Printf("Error details %v:\n", err.Error())
		panic(err)
	}
	defer f.Close()

	log.Printf("Input file opened.\n")
	reader := csv.NewReader(f)
	_, err = reader.Read()
	if err != nil {
		fmt.Printf("Error reading csv headers.")
		panic(err)
	}
	for !end {
		b := Bulk{}
		log.Printf("Beginning bulk.")
		for i := 0; i < 1000; i++ {

			row, err := reader.Read()
			if err != nil {
				if err == io.EOF {
					end = true
					break
				}
				fmt.Printf("Error reading in csv: %v\n", err.Error())
				fmt.Printf("%v lines read before error \n", numberStarted)
				panic(err)
			}

			log.Printf("Read row %v\n", row)
			if len(row) == 0 {
				continue
			}

			numberStarted++

			log.Printf("Tranlsating Row")

			toSend, err := translateRow(row)
			if err != nil {
				log.Printf("Failed on row %+v with error %+v", row, err.Error())
				continue
			}

			b.Items = append(b.Items, toSend)
			log.Printf("Send bulk with %v items.", len(b.Items))
		}

		log.Printf("Send bulk with %v items.", len(b.Items))
		toSendString := `{ "index" :{} }` + "\n"

		byteArray := []byte{}

		for i := 0; i < len(b.Items); i++ {
			bytesToSend, err := json.Marshal(b.Items[i])
			if err != nil {
				log.Printf("Failed marshalling batch.")
				continue
			}
			bytesToSend = append(bytesToSend, []byte("\n")...)

			byteArray = append(byteArray, []byte(toSendString)...)
			byteArray = append(byteArray, bytesToSend...)
		}

		fmt.Printf("Request: %s\n", byteArray)
		req, _ := http.NewRequest("PUT", config.ESAddress, bytes.NewBuffer(byteArray))

		client := &http.Client{}
		resp, err := client.Do(req)

		if err != nil {
			log.Printf("Failed Posting to ES: %v.", err.Error())
			continue
		} else {
			log.Printf("Posted with response code %v", resp.StatusCode)
			response, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				panic(err)
			}
			fmt.Printf("%s\n", response)
		}
	}
}
