package core

import (
	"os"
	"fmt"
	"time"
	"bufio"
	"bytes"
	"github.com/streadway/amqp"
	"github.com/LaCumbancha/reviews-analysis/cmd/nodes/inputs/business-scatter/rabbitmq"

	log "github.com/sirupsen/logrus"
	props "github.com/LaCumbancha/reviews-analysis/cmd/common/properties"
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
	outputQueue 		*rabbitmq.RabbitOutputQueue
}

func NewScatter(config ScatterConfig) *Scatter {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%s/", config.RabbitIp, config.RabbitPort))
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ at (%s, %s). Err: '%s'", config.RabbitIp, config.RabbitPort, err)
	} else {
		log.Infof("Connected to RabbitMQ at (%s, %s).", config.RabbitIp, config.RabbitPort)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a RabbitMQ channel. Err: '%s'", err)
	} else {
		log.Infof("RabbitMQ channel opened.")
	}

	scatterDirect := rabbitmq.NewRabbitOutputQueue(props.BusinessesScatterOutput, config.CitbizMappers, ch)
	
	scatter := &Scatter {
		data: 				config.Data,
		connection:			conn,
		channel:			ch,
		bulkSize:			config.BulkSize,
		poolSize:			config.WorkersPool,
		innerChannel:		make(chan string),
		outputQueue:		scatterDirect,
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
            scatter.outputQueue.PublishBulk(bulkNumber, bulk[:len(bulk)-1])

            buffer = bytes.NewBufferString("")
            chunkNumber = 0
        }
    }

    if err := scanner.Err(); err != nil {
        log.Fatalf("Error reading businesses data from file %s. Err: '%s'", scatter.data, err)
    }

    // Publishing end messages.
    scatter.outputQueue.PublishFinish()

    log.Infof("Time: %s.", time.Now().Sub(start).String())
}

func (scatter *Scatter) Stop() {
	log.Infof("Closing Businesses-Scatter connections.")
	scatter.connection.Close()
	scatter.channel.Close()
}
