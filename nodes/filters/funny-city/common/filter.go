package common

import (
	"fmt"
	"sync"
	"encoding/json"
	"github.com/streadway/amqp"
	"github.com/LaCumbancha/reviews-analysis/nodes/filters/funny-city/rabbitmq"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/nodes/filters/funny-city/logger"
)

type FilterConfig struct {
	Instance			string
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
	outputQueue := rabbitmq.NewRabbitOutputQueue(rabbitmq.OUTPUT_QUEUE_NAME, config.Instance, ch)
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
	var endSignals = make(map[string]int)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		bulkCounter := 0
		for message := range filter.inputQueue.ConsumeData() {
			messageBody := string(message.Body)

			if rabbitmq.IsEndMessage(messageBody) {
				filter.processEndSignal(messageBody, endSignals, endSignalsMutex, &wg)
			} else {
				bulkCounter++
				logb.Instance().Infof(fmt.Sprintf("Funcit data bulk #%d received.", bulkCounter), bulkCounter)

				wg.Add(1)
				go func(bulkNumber int) {
					filter.calculator.Save(bulkNumber, messageBody)
					wg.Done()
				}(bulkCounter)
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

func (filter *Filter) processEndSignal(newMessage string, endSignals map[string]int, mutex *sync.Mutex, wg *sync.WaitGroup) {
	mutex.Lock()
	endSignals[newMessage] = endSignals[newMessage] + 1
	newSignal := endSignals[newMessage] == 1
	signalsReceived := len(endSignals)
	mutex.Unlock()

	if newSignal {
		log.Infof("End-Message #%d received.", signalsReceived)
	}

	// Waiting for the total needed End-Signals to send the own End-Message.
	if (signalsReceived == filter.endSignals) && newSignal {
		log.Infof("All End-Messages were received.")
		wg.Done()
	}
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
