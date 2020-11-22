package common

import (
	"fmt"
	"sync"
	"encoding/json"
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/filters/user/rabbitmq"
)

type FilterConfig struct {
	RabbitIp			string
	RabbitPort			string
	UserAggregators		int
}

type Filter struct {
	connection 		*amqp.Connection
	channel 		*amqp.Channel
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
		inputQueue:		inputQueue,
		outputQueue:	outputQueue,
		endSignals:		config.UserAggregators,
	}

	return filter
}

func (filter *Filter) Run() {
	log.Infof("Starting to listen for user reviews data.")

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
					log.Infof("All End-Message were received.")
					wg.Done()
				}
				
				//rabbitmq.AckMessage(&message, rabbitmq.END_MESSAGE)
			} else {
				log.Infof("Data '%s' received.", messageBody)

				wg.Add(1)
				go func() {
					filter.filterFunnyBusiness(messageBody)
					//rabbitmq.AckMessage(&message, utils.GetReviewId(review))
					wg.Done()
				}()
			}
		}
	}()
	
    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are sent.
    wg.Wait()

    // Publishing end messages.
    filter.outputQueue.PublishFinish()
}

func (filter *Filter) filterFunnyBusiness(rawData string) {
	var mappedUserData rabbitmq.UserData
	json.Unmarshal([]byte(rawData), &mappedUserData)

	if (mappedUserData.Reviews > 50) {
		data, err := json.Marshal(mappedUserData)
		if err != nil {
			log.Errorf("Error generating Json from (%s). Err: '%s'", mappedUserData, err)
		}
		filter.outputQueue.PublishData(data, mappedUserData.UserId)
	} else {
		log.Infof("Data '%s' filtered due to not having enough reviews.", rawData)
	}
}

func (filter *Filter) Stop() {
	log.Infof("Closing User Filter connections.")
	filter.connection.Close()
	filter.channel.Close()
}
