package common

import (
	"fmt"
	"sync"
	"encoding/json"
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/filters/funny-city/rabbitmq"
)

type FilterConfig struct {
	RabbitIp			string
	RabbitPort			string
	FuncitAggregators	int
}

type Filter struct {
	connection 		*amqp.Connection
	channel 		*amqp.Channel
	calculator		*Calculator
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
		calculator:		NewCalculator(),
		inputQueue:		inputQueue,
		outputQueue:	outputQueue,
		endSignals:		config.FuncitAggregators,
	}

	return filter
}

func (filter *Filter) Run() {
	log.Infof("Starting to listen for funny-city data.")

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
					filter.calculator.Save(messageBody)
					//rabbitmq.AckMessage(&message, utils.GetReviewId(review))
					wg.Done()
				}()
			}
		}
	}()
	
    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are received.
    wg.Wait()

    for _, topTenData := range filter.calculator.RetrieveTopTen() {
		wg.Add(1)
		go filter.sendTopTenData(topTenData, &wg)
	}

    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are sent.
    wg.Wait()

    // Sending End-Message to consumers.
    filter.outputQueue.PublishFinish()
}

func (filter *Filter) sendTopTenData(topTenData rabbitmq.FunnyCityData, wg *sync.WaitGroup) {
	data, err := json.Marshal(topTenData)
	if err != nil {
		log.Errorf("Error generating Json from (%s). Err: '%s'", topTenData, err)
	} else {
		filter.outputQueue.PublishData(data)
	}
	wg.Done()
}

func (filter *Filter) Stop() {
	log.Infof("Closing Funny-City Filter connections.")
	filter.connection.Close()
	filter.channel.Close()
}
