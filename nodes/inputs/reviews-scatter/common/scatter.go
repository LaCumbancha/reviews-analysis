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

type ReviewsScatterConfig struct {
	Data				string
	RabbitIp			string
	RabbitPort			string
	FunbizMappers		int
	WeekdaysMappers		int
	HashesMappers		int
	UsersMappers		int
	StarsMappers		int
}

type ReviewsScatter struct {
	data 				string
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	outputDirect 		*rabbitmq.RabbitOutputDirect
}

func NewReviewsScatter(config ReviewsScatterConfig) *ReviewsScatter {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%s/", config.RabbitIp, config.RabbitPort))
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ at (%s, %s). Err: '%s'", config.RabbitIp, config.RabbitPort , err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a RabbitMQ channel. Err: '%s'", err)
	}

	scatterDirect := rabbitmq.NewRabbitOutputDirect(
		rabbitmq.OUTPUT_EXCHANGE_NAME, 
		config.FunbizMappers, 
		config.WeekdaysMappers, 
		config.HashesMappers, 
		config.UsersMappers, 
		config.StarsMappers, 
		ch,
	)
	
	scatter := &ReviewsScatter {
		data: 				config.Data,
		connection:			conn,
		channel:			ch,
		outputDirect:		scatterDirect,
	}

	return scatter
}

func (scatter *ReviewsScatter) Run() {
	file, err := os.Open(scatter.data)
    if err != nil {
        log.Fatalf("Error opening reviews data file. Err: '%s'", err)
    }
    defer file.Close()

	var wg sync.WaitGroup
    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
    	review := scanner.Text()

    	// Publishing asynchronously with Goroutines.
    	wg.Add(1)
    	go func() {
    		reviewId := utils.GetReviewId(review)
    		scatter.outputDirect.PublishReview(review, reviewId)
    		wg.Done()
    	}()
    }

    if err := scanner.Err(); err != nil {
        log.Fatalf("Error reading reviews data from file %s. Err: '%s'", scatter.data, err)
    }

    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are sent.
    wg.Wait()

    // Publishing end message.
    scatter.outputDirect.PublishFinish()
}

func (scatter *ReviewsScatter) Stop() {
	log.Infof("Closing Reviews-Scatter connections.")
	scatter.connection.Close()
	scatter.channel.Close()
}
