package common

import (
	"fmt"
	"sync"
	"encoding/json"
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/filters/distinct-hash/rabbitmq"
)

type FilterConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	MinReviews			int
	DishashAggregators	int
	DishashJoiners		int
}

type Filter struct {
	connection 		*amqp.Connection
	channel 		*amqp.Channel
	minReviews 		int
	inputQueue 		*rabbitmq.RabbitInputQueue
	outputDirect	*rabbitmq.RabbitOutputDirect
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
	outputDirect := rabbitmq.NewRabbitOutputDirect(rabbitmq.OUTPUT_EXCHANGE_NAME, config.Instance, config.DishashJoiners, ch)
	filter := &Filter {
		connection:		conn,
		channel:		ch,
		minReviews:		config.MinReviews,
		inputQueue:		inputQueue,
		outputDirect:	outputDirect,
		endSignals:		config.DishashAggregators,
	}

	return filter
}

func (filter *Filter) Run() {
	log.Infof("Starting to listen for users distinct text-hashes.")

	var endSignalsMutex = &sync.Mutex{}
	var endSignals = make(map[string]int)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for message := range filter.inputQueue.ConsumeData() {
			messageBody := string(message.Body)

			if rabbitmq.IsEndMessage(messageBody) {
				filter.processEndSignal(messageBody, endSignals, endSignalsMutex, &wg)
				//rabbitmq.AckMessage(&message, rabbitmq.END_MESSAGE)
			} else {
				log.Infof("Data '%s' received.", messageBody)

				wg.Add(1)
				go func() {
					filter.filterRepeatedTexts(messageBody)
					//rabbitmq.AckMessage(&message, utils.GetReviewId(review))
					wg.Done()
				}()
			}
		}
	}()
	
    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are sent.
    wg.Wait()

    // Publishing end messages.
    filter.outputDirect.PublishFinish()
}

func (filter *Filter) processEndSignal(newMessage string, endSignals map[string]int, mutex *sync.Mutex, wg *sync.WaitGroup) {
	mutex.Lock()
	endSignals[newMessage] = endSignals[newMessage] + 1
	newSignal := endSignals[newMessage] == 1
	signalsReceived := len(endSignals)
	mutex.Unlock()

	log.Infof("End-Message #%d received.", signalsReceived)

	// Waiting for the total needed End-Signals to send the own End-Message.
	if (signalsReceived == filter.endSignals) && newSignal {
		log.Infof("All End-Messages were received.")
		wg.Done()
	}
}

func (filter *Filter) filterRepeatedTexts(rawData string) {
	var distinctHashesData rabbitmq.DistinctHashesData
	json.Unmarshal([]byte(rawData), &distinctHashesData)

	if (distinctHashesData.Distinct == 1) {
		data, err := json.Marshal(distinctHashesData)
		if err != nil {
			log.Errorf("Error generating Json from (%s). Err: '%s'", distinctHashesData, err)
		}
		filter.outputDirect.PublishData(data, distinctHashesData.UserId)
	} else {
		log.Infof("Data '%s' filtered due to different texts.", rawData)
	}
}

func (filter *Filter) Stop() {
	log.Infof("Closing Distinct-Hash Filter connections.")
	filter.connection.Close()
	filter.channel.Close()
}
