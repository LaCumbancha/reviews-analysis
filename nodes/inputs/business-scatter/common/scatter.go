package common

import (
	"os"
	"fmt"
	"sync"
	"bufio"
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/inputs/business-scatter/utils"
	"github.com/LaCumbancha/reviews-analysis/nodes/inputs/business-scatter/rabbitmq"
)

type ScatterConfig struct {
	Data				string
	RabbitIp			string
	RabbitPort			string
	WorkersPool 		int
	CitbizMappers		int
}

type Scatter struct {
	data 				string
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	innerChannel		chan string
	poolSize			int
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
		innerChannel:		make(chan string),
		poolSize:			config.WorkersPool,
		outputQueue:		scatterDirect,
	}

	return scatter
}

func (scatter *Scatter) Run() {
	var wg sync.WaitGroup
	wg.Add(1)
	go scatter.retrieveBusinesses(&wg)

	log.Infof("Initializing scatter with %d workers.", scatter.poolSize)
	for worker := 1 ; worker <= scatter.poolSize ; worker++ {
		go func() {
			log.Infof("Initializing worker %d.", worker)
			for business := range scatter.innerChannel {
				businessId := utils.GetBusinessId(business)
				log.Infof("Processing business %s.", businessId)
    			scatter.outputQueue.PublishBusiness(business, businessId)
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
	for scanner.Scan() {
		wg.Add(1)
    	review := scanner.Text()
    	scatter.innerChannel <- review
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
