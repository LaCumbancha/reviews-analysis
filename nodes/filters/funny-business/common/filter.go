package common

import (
	"fmt"
	"sync"
	"encoding/json"
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/filters/funny-business/rabbitmq"
)

type FilterConfig struct {
	RabbitIp			string
	RabbitPort			string
	FunbizMappers 		int
	FunbizAggregators	int
}

type Filter struct {
	connection 		*amqp.Connection
	channel 		*amqp.Channel
	inputQueue 		*rabbitmq.RabbitInputQueue
	outputDirect 	*rabbitmq.RabbitOutputDirect
	endSignals		int
}

func NewFilter(config FilterConfig) *Filter {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%s/", config.RabbitIp, config.RabbitPort))
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ at (%s, %s). Err: '%s'", config.RabbitIp, config.RabbitPort , err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a RabbitMQ channel. Err: '%s'", err)
	}

	inputQueue := rabbitmq.NewRabbitInputQueue(rabbitmq.INPUT_QUEUE_NAME, ch)
	outputDirect := rabbitmq.NewRabbitOutputDirect(rabbitmq.OUTPUT_EXCHANGE_NAME, config.FunbizAggregators, ch)
	filter := &Filter {
		connection:		conn,
		channel:		ch,
		inputQueue:		inputQueue,
		outputDirect:	outputDirect,
		endSignals:		config.FunbizMappers,
	}

	return filter
}

func (filter *Filter) Run() {
	log.Infof("Starting to listen for funny-business data.")

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

				if (endSignalsReceived >= filter.endSignals) {
					log.Infof("End-Message received.")

					// Using WaitGroup to avoid publishing finish message before all others are sent.
					wg.Done()
					wg.Wait()
	
					filter.outputDirect.PublishFinish()
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
}

func (filter *Filter) filterFunnyBusiness(rawData string) {
	var mappedFunnyBusiness rabbitmq.FunnyBusinessData
	json.Unmarshal([]byte(rawData), &mappedFunnyBusiness)

	if (mappedFunnyBusiness.Funny > 0) {
		data, err := json.Marshal(mappedFunnyBusiness)
		if err != nil {
			log.Errorf("Error generating Json from (%s). Err: '%s'", mappedFunnyBusiness, err)
		}
		filter.outputDirect.PublishData(data, mappedFunnyBusiness.BusinessId)
	} else {
		log.Infof("Data '%s' filtered due to it's lack of funniness.", rawData)
	}
}

func (filter *Filter) Stop() {
	log.Infof("Closing Funny-Business Filter connections.")
	filter.connection.Close()
	filter.channel.Close()
}
