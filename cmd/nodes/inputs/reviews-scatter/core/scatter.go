package core

import (
	"os"
	"time"
	"bufio"
	"bytes"
	"github.com/streadway/amqp"

	log "github.com/sirupsen/logrus"
	props "github.com/LaCumbancha/reviews-analysis/cmd/common/properties"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
	rabbit "github.com/LaCumbancha/reviews-analysis/cmd/common/middleware"
)

type ScatterConfig struct {
	Instance			string
	Data				string
	RabbitIp			string
	RabbitPort			string
	BulkSize			int
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
	outputDirect 		*rabbit.RabbitOutputDirect
	outputSignals		map[string]int
}

func NewScatter(config ScatterConfig) *Scatter {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	outputDirect := rabbit.NewRabbitOutputDirect(channel, props.ReviewsScatterOutput, comms.EndMessage(config.Instance))
	
	scatter := &Scatter {
		data: 				config.Data,
		connection:			connection,
		channel:			channel,
		bulkSize:			config.BulkSize,
		outputDirect:		outputDirect,
		outputSignals:		GenerateSignalsMap(config.FunbizMappers, config.WeekdaysMappers, config.HashesMappers, config.UsersMappers, config.StarsMappers),
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
            trimmedBulk := bulk[:len(bulk)-1]

            for _, partition := range PartitionableValues {
            	scatter.outputDirect.PublishData([]byte(trimmedBulk), partition)
            }

            buffer = bytes.NewBufferString("")
            chunkNumber = 0
        }
    }

    if err := scanner.Err(); err != nil {
        log.Fatalf("Error reading reviews data from file %s. Err: '%s'", scatter.data, err)
    }

    // Publishing end messages.
    for _, partition := range PartitionableValues {
    	for idx := 0 ; idx < scatter.outputSignals[partition]; idx++ {
    		scatter.outputDirect.PublishFinish(partition)
    	}
    }

    log.Infof("Time: %s.", time.Now().Sub(start).String())
}

func (scatter *Scatter) Stop() {
	log.Infof("Closing Reviews-Scatter connections.")
	scatter.connection.Close()
	scatter.channel.Close()
}
