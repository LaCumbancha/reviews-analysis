package common

import (
	"os"
	"fmt"
	"sync"
	"bufio"
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/inputs/business-scatter/rabbitmq"
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

	scatterDirect := rabbitmq.NewRabbitOutputQueue(rabbitmq.OUTPUT_QUEUE_NAME, config.CitbizMappers, ch)
	
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
	var wg sync.WaitGroup
	wg.Add(1)
	go scatter.retrieveBusinesses(&wg)

	bulkMutex := &sync.Mutex{}
	bulkNumber := 0

	log.Infof("Initializing scatter with %d workers.", scatter.poolSize)
	for worker := 1 ; worker <= scatter.poolSize ; worker++ {
		log.Infof("Initializing worker %d.", worker)

		go func() {
			for bulk := range scatter.innerChannel {
				bulkMutex.Lock()
				bulkNumber++
				innerBulk := bulkNumber
				bulkMutex.Unlock()

    			scatter.outputQueue.PublishBulk(innerBulk, bulk)
    			wg.Done()
			}
		}()
	}

    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are sent.
    wg.Wait()

    // Publishing end messages.
    scatter.outputQueue.PublishFinish()
}

func (scatter *Scatter) retrieveBusinesses(wg *sync.WaitGroup) {
	log.Infof("Starting to load businesses.")

	file, err := os.Open(scatter.data)
    if err != nil {
        log.Fatalf("Error opening businesses data file. Err: '%s'", err)
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)

	wg.Add(1)
	bulk := ""
	chunkNumber := 0
	for scanner.Scan() {
		bulk = bulk + scanner.Text() + "\n"
		chunkNumber++

		if chunkNumber == scatter.bulkSize {
			scatter.innerChannel <- bulk[:len(bulk)-1]
			wg.Add(1)

			bulk = ""
			chunkNumber = 0
		}
	}

	if bulk != "" {
    	scatter.innerChannel <- bulk[:len(bulk)-1]
    } else {
    	wg.Done()
    }

    if err := scanner.Err(); err != nil {
        log.Fatalf("Error reading businesses data from file %s. Err: '%s'", scatter.data, err)
    }

    wg.Done()
}

func (scatter *Scatter) Stop() {
	log.Infof("Closing Business-Scatter connections.")
	scatter.connection.Close()
	scatter.channel.Close()
}
