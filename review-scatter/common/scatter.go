package common

import (
	"os"
	"fmt"
	"sync"
	"bufio"

	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/review-analysis/review-scatter/rabbitmq"
)

type ReviewsScatterConfig struct {
	Data				string
	RabbitIp			string
	RabbitPort			string
	ScatterQueueName	string
}

type ReviewsScatter struct {
	connection 		*amqp.Connection
	channel 		*amqp.Channel
	data 			string
	queue 			*rabbitmq.RabbitQueue
}

func NewReviewsScatter(config ReviewsScatterConfig) *ReviewsScatter {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%s/", config.RabbitIp, config.RabbitPort))
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ at (%s, %s).", config.RabbitIp, config.RabbitPort , err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a RabbitMQ channel.", err)
	}

	scatterQueue := rabbitmq.NewRabbitQueue(config.ScatterQueueName, ch)
	scatter := &ReviewsScatter {
		connection:		conn,
		channel:		ch,
		data: 			config.Data,
		queue:			scatterQueue,
	}

	return scatter
}

func (scatter *ReviewsScatter) Run() {
	file, err := os.Open(scatter.data)
    if err != nil {
        log.Fatalf("Error opening reviews data file.", err)
    }
    defer file.Close()

	var wg sync.WaitGroup
    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
    	review := scanner.Text()

    	// Publishing asynchronously with Goroutines.
    	go func() {
    		wg.Add(1)
    		scatter.queue.Publish(GetReviewId(review), review)
    		wg.Done()
    	}()
    }

    if err := scanner.Err(); err != nil {
        log.Fatalf("Error reading reviews data from file %s.", scatter.data, err)
    }

    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are sent.
    wg.Wait()
}

func (scatter *ReviewsScatter) Stop() {
	scatter.connection.Close()
	scatter.channel.Close()
}

func GetReviewId(review string) string {
	// Substring limits hardcoded (after analysing data) to avoid parsing the review.
	return review[14:34]
}
