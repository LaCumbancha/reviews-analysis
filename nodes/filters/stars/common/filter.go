package common

import (
	"fmt"
	"sync"
	"encoding/json"
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/filters/stars/logging"
	"github.com/LaCumbancha/reviews-analysis/nodes/filters/stars/rabbitmq"
)

type FilterConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	StarsMappers		int
	StarsAggregators	int
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
	outputDirect := rabbitmq.NewRabbitOutputDirect(rabbitmq.OUTPUT_EXCHANGE_NAME, config.Instance, config.StarsAggregators, ch)
	filter := &Filter {
		connection:		conn,
		channel:		ch,
		inputQueue:		inputQueue,
		outputDirect:	outputDirect,
		endSignals:		config.StarsMappers,
	}

	return filter
}

func (filter *Filter) Run() {
	log.Infof("Starting to listen for user stars data.")

	var endSignalsMutex = &sync.Mutex{}
	var endSignals = make(map[string]int)

	bulkMutex := &sync.Mutex{}
	bulkCounter := 0

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for message := range filter.inputQueue.ConsumeData() {
			messageBody := string(message.Body)

			if rabbitmq.IsEndMessage(messageBody) {
				filter.processEndSignal(messageBody, endSignals, endSignalsMutex, &wg)
			} else {
				bulkMutex.Lock()
				bulkCounter++
				innerBulk := bulkCounter
				bulkMutex.Unlock()

				logging.Infof(fmt.Sprintf("Funbiz data bulk #%d received.", innerBulk), innerBulk)

				wg.Add(1)
				go func(bulkNumber int) {
					filter.filterLowStars(bulkNumber, messageBody)
					wg.Done()
				}(innerBulk)
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

func (filter *Filter) filterLowStars(bulkNumber int, rawStarsDataBulk string) {
	var starsDataList []rabbitmq.StarsData
	json.Unmarshal([]byte(rawStarsDataBulk), &starsDataList)

	for _, starsData := range starsDataList {

		if (starsData.Stars == 5) {
			data, err := json.Marshal(starsData)
			if err != nil {
				log.Errorf("Error generating Json from (%s). Err: '%s'", starsData, err)
			}

			filter.outputDirect.PublishData(data, starsData.UserId)
		} else {
			log.Infof("Data '%s' filtered due to not having enough stars.", starsData)
		}

	}
}

func (filter *Filter) Stop() {
	log.Infof("Closing Stars Filter connections.")
	filter.connection.Close()
	filter.channel.Close()
}
