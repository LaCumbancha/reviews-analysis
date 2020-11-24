package common

import (
	"os"
	"fmt"
	"sync"
	"bufio"
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/inputs/reviews-scatter/utils"
	"github.com/LaCumbancha/reviews-analysis/nodes/inputs/reviews-scatter/rabbitmq"
)

type ScatterConfig struct {
	Instance			string
	Data				string
	RabbitIp			string
	RabbitPort			string
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
	innerChannel		chan string
	poolSize			int
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
		innerChannel:		make(chan string),
		poolSize:			config.WorkersPool,
		outputDirect:		scatterDirect,
	}

	return scatter
}

func (scatter *Scatter) Run() {

	var wg sync.WaitGroup
	go scatter.retrieveReviews(&wg)
	log.Infof("Initializing scatter with %d workers.", scatter.poolSize)

	for worker := 1 ; worker <= scatter.poolSize ; worker++ {
		go func() {
			log.Infof("Initializing worker %d.", worker)
			for review := range scatter.innerChannel {
				reviewId := utils.GetReviewId(review)
				log.Infof("Processing review %s.", reviewId)
    			scatter.outputDirect.PublishReview(review, reviewId)
    			wg.Done()
			}
		}()
	}

    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are sent.
    wg.Wait()

    // Publishing end messages.
    scatter.outputDirect.PublishFinish()
}

func (scatter *Scatter) retrieveReviews(wg *sync.WaitGroup) {
	log.Infof("Starting to load reviews.")

	file, err := os.Open(scatter.data)
    if err != nil {
        log.Fatalf("Error opening reviews data file. Err: '%s'", err)
    }
    defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		wg.Add(1)
    	review := scanner.Text()
    	scatter.innerChannel <- review
    }

    if err := scanner.Err(); err != nil {
        log.Fatalf("Error reading reviews data from file %s. Err: '%s'", scatter.data, err)
    }
}

func (scatter *Scatter) Stop() {
	log.Infof("Closing Reviews-Scatter connections.")
	scatter.connection.Close()
	scatter.channel.Close()
}
