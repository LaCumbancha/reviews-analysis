package common

import (
	"fmt"
	"sync"
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/prettiers/funniest-cities/rabbitmq"
)

type FilterConfig struct {
	RabbitIp		string
	RabbitPort		string
	FuncitFilters	int
}

type Filter struct {
	connection 		*amqp.Connection
	channel 		*amqp.Channel
	builder		*Builder
	inputQueue 		*rabbitmq.RabbitInputQueue
	outputQueue 	*rabbitmq.RabbitOutputQueue
	endSignals		int
}

func NewFilter(config FilterConfig) *Filter {
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

	inputQueue := rabbitmq.NewRabbitInputQueue(rabbitmq.INPUT_QUEUE_NAME, ch)
	outputQueue := rabbitmq.NewRabbitOutputQueue(rabbitmq.OUTPUT_QUEUE_NAME, ch)
	filter := &Filter {
		connection:		conn,
		channel:		ch,
		builder:		NewBuilder(),
		inputQueue:		inputQueue,
		outputQueue:	outputQueue,
		endSignals:		config.FuncitFilters,
	}

	return filter
}

func (filter *Filter) Run() {
	log.Infof("Starting to listen for top funniest-cities data.")

	var endSignalsMutex = &sync.Mutex{}
	var endSignalsReceived = 0

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for message := range filter.inputQueue.ConsumeData() {
			messageBody := string(message.Body)

			if messageBody == rabbitmq.END_MESSAGE {
				// Waiting for the total needed End-Signals to send the own End-Message.
				endSignalsMutex.Lock()
				endSignalsReceived++
				endSignalsMutex.Unlock()
				log.Infof("End-Message #%d received.", endSignalsReceived)

				if (endSignalsReceived == filter.endSignals) {
					log.Infof("All End-Messages were received.")
					wg.Done()
				}
				
				//rabbitmq.AckMessage(&message, rabbitmq.END_MESSAGE)
			} else {
				log.Infof("Data '%s' received.", messageBody)

				wg.Add(1)
				go func() {
					filter.builder.Save(messageBody)
					//rabbitmq.AckMessage(&message, utils.GetReviewId(review))
					wg.Done()
				}()
			}
		}
	}()
	
    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are received.
    wg.Wait()

    filter.sendResults()

    // Sending End-Message to consumers.
    filter.outputQueue.PublishFinish()
}

func (filter *Filter) sendResults() {
	results := filter.builder.BuildTopTen()
	filter.outputQueue.PublishData(fmt.Sprintf("Top 10 Funniest Cities --- %s", results))
}

func (filter *Filter) Stop() {
	log.Infof("Closing Funniest-Cities Prettier connections.")
	filter.connection.Close()
	filter.channel.Close()
}
