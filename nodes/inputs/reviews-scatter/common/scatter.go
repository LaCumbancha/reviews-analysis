package common

import (
	"os"
	"fmt"
	"time"
	"bufio"
	"bytes"
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/inputs/reviews-scatter/rabbitmq"
)

type ScatterConfig struct {
	Instance			string
	Data				string
	RabbitIp			string
	RabbitPort			string
	BulkSize			int
	WorkersPool 		int
	FunbizMappers		int
	WeekdaysMappers		int
	HashesMappers		int
	UsersMappers		int
	StarsMappers		int
}

type Scatter struct {
	data 				string
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	bulkSize			int
	poolSize			int
	innerChannel		chan string
	outputDirect 		*rabbitmq.RabbitOutputDirect
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

	scatterDirect := rabbitmq.NewRabbitOutputDirect(
		rabbitmq.OUTPUT_EXCHANGE_NAME,
		config.Instance,
		config.FunbizMappers, 
		config.WeekdaysMappers, 
		config.HashesMappers, 
		config.UsersMappers, 
		config.StarsMappers, 
		ch,
	)
	
	scatter := &Scatter {
		data: 				config.Data,
		connection:			conn,
		channel:			ch,
		bulkSize:			config.BulkSize,
		poolSize:			config.WorkersPool,
		innerChannel:		make(chan string),
		outputDirect:		scatterDirect,
	}

	return scatter
}

func (scatter *Scatter) Run() {
	start := time.Now()

	log.Infof("Starting to load reviews.")

    file, err := os.Open(scatter.data)
    if err != nil {
        log.Fatalf("Error opening reviews data file. Err: '%s'", err)
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
            scatter.outputDirect.PublishBulk(bulkNumber, bulk[:len(bulk)-1])

            buffer = bytes.NewBufferString("")
            chunkNumber = 0
        }
    }

    if err := scanner.Err(); err != nil {
        log.Fatalf("Error reading reviews data from file %s. Err: '%s'", scatter.data, err)
    }

    // Publishing end messages.
    scatter.outputDirect.PublishFinish()

    log.Infof("Time: %s.", time.Now().Sub(start).String())
}

func (scatter *Scatter) Stop() {
	log.Infof("Closing Reviews-Scatter connections.")
	scatter.connection.Close()
	scatter.channel.Close()
}
