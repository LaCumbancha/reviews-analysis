package core

import (
	"os"
	"fmt"
	"time"
	"bufio"
	"bytes"
	"github.com/streadway/amqp"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	props "github.com/LaCumbancha/reviews-analysis/cmd/common/properties"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
	rabbit "github.com/LaCumbancha/reviews-analysis/cmd/common/middleware"
)

type ScatterConfig struct {
	Data				string
	RabbitIp			string
	RabbitPort			string
	BulkSize			int
	WorkersPool 		int
	CitbizMappers		int
}

type Scatter struct {
	data 				string
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	bulkSize 			int
	poolSize			int
	innerChannel		chan string
	outputQueue 		*rabbit.RabbitOutputQueue
}

func NewScatter(config ScatterConfig) *Scatter {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	scatterQueue := rabbit.NewRabbitOutputQueue(channel, props.BusinessesScatterOutput, comms.EndMessage(""), comms.EndSignals(config.CitbizMappers))

	scatter := &Scatter {
		data: 				config.Data,
		connection:			connection,
		channel:			channel,
		bulkSize:			config.BulkSize,
		poolSize:			config.WorkersPool,
		innerChannel:		make(chan string),
		outputQueue:		scatterQueue,
	}

	return scatter
}

func (scatter *Scatter) Run() {
	start := time.Now()

	log.Infof("Starting to load businesses.")

    file, err := os.Open(scatter.data)
    if err != nil {
        log.Fatalf("Error opening businesses data file. Err: '%s'", err)
    }
    defer file.Close()

    bulkNumber := 0
    chunkNumber := 0
    scanner := bufio.NewScanner(file)
    buffer := bytes.NewBufferString("")
    for scanner.Scan() {
        buffer.WriteString(scanner.Text())
        buffer.WriteString("\n")

        chunkNumber++
        if chunkNumber == scatter.bulkSize {
            bulkNumber++
            bulk := buffer.String()
            scatter.sendData(bulkNumber, bulk[:len(bulk)-1])

            chunkNumber = 0
            buffer = bytes.NewBufferString("")
        }
    }

    if err := scanner.Err(); err != nil {
        log.Fatalf("Error reading businesses data from file %s. Err: '%s'", scatter.data, err)
    }

    bulkNumber++
    bulk := buffer.String()
    if bulk != "" {
    	scatter.sendData(bulkNumber, bulk[:len(bulk)-1])
    }

    // Publishing end messages.
    scatter.outputQueue.PublishFinish()

    log.Infof("Time: %s.", time.Now().Sub(start).String())
}

func (scatter *Scatter) sendData(bulkNumber int, bulk string) {
	err := scatter.outputQueue.PublishData([]byte(bulk))

	if err != nil {
		log.Errorf("Error sending businesses bulk #%d to output queue %s. Err: '%s'", bulkNumber, scatter.outputQueue.Name, err)
	} else {
		logb.Instance().Infof(fmt.Sprintf("Businesses bulk #%d sent to output queue %s.", bulkNumber, scatter.outputQueue.Name), bulkNumber)
	}
}

func (scatter *Scatter) Stop() {
	log.Infof("Closing Businesses-Scatter connections.")
	scatter.connection.Close()
	scatter.channel.Close()
}
