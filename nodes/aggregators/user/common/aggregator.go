package common

import (
	"fmt"
	"sync"
	"encoding/json"
	"github.com/streadway/amqp"
	"github.com/LaCumbancha/reviews-analysis/nodes/aggregators/user/rabbitmq"
	
	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/nodes/aggregators/user/logger"
)

type AggregatorConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	InputTopic			string
	UserMappers 		int
	UserFilters 		int
	BotUserFilters		int
}

type Aggregator struct {
	connection 		*amqp.Connection
	channel 		*amqp.Channel
	calculator		*Calculator
	inputDirect 	*rabbitmq.RabbitInputDirect
	outputQueue1 	*rabbitmq.RabbitOutputQueue
	outputQueue2	*rabbitmq.RabbitOutputQueue
	endSignals		int
}

func NewAggregator(config AggregatorConfig) *Aggregator {
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

	inputDirect := rabbitmq.NewRabbitInputDirect(rabbitmq.INPUT_EXCHANGE_NAME, config.InputTopic, ch)
	outputQueue1 := rabbitmq.NewRabbitOutputQueue(rabbitmq.OUTPUT_QUEUE1_NAME, config.Instance, config.UserFilters, ch)
	outputQueue2 := rabbitmq.NewRabbitOutputQueue(rabbitmq.OUTPUT_QUEUE2_NAME, config.Instance, config.BotUserFilters, ch)
	aggregator := &Aggregator {
		connection:		conn,
		channel:		ch,
		calculator:		NewCalculator(),
		inputDirect:	inputDirect,
		outputQueue1:	outputQueue1,
		outputQueue2:	outputQueue2,
		endSignals:		config.UserMappers,
	}

	return aggregator
}

func (aggregator *Aggregator) Run() {
	log.Infof("Starting to listen for user reviews data.")

	var endSignalsMutex = &sync.Mutex{}
	var endSignals = make(map[string]int)

	bulkMutex := &sync.Mutex{}
	bulkCounter := 0

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for message := range aggregator.inputDirect.ConsumeData() {
			messageBody := string(message.Body)

			if rabbitmq.IsEndMessage(messageBody) {
				aggregator.processEndSignal(messageBody, endSignals, endSignalsMutex, &wg)
			} else {
				bulkMutex.Lock()
				bulkCounter++
				innerBulk := bulkCounter
				bulkMutex.Unlock()

				logb.Instance().Infof(fmt.Sprintf("Weekday data bulk #%d received.", innerBulk), innerBulk)

				wg.Add(1)
				go func(bulkNumber int) {
					aggregator.calculator.Aggregate(bulkNumber, messageBody)
					wg.Done()
				}(innerBulk)
			}
		}
	}()
	
    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are received.
    wg.Wait()

    for _, aggregatedData := range aggregator.calculator.RetrieveData() {
		wg.Add(1)
		go aggregator.sendAggregatedData(aggregatedData, &wg)
	}

    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are sent.
    wg.Wait()

    // Sending End-Message to consumers.
    aggregator.outputQueue1.PublishFinish()
    aggregator.outputQueue2.PublishFinish()
}

func (aggregator *Aggregator) processEndSignal(newMessage string, endSignals map[string]int, mutex *sync.Mutex, wg *sync.WaitGroup) {
	mutex.Lock()
	endSignals[newMessage] = endSignals[newMessage] + 1
	newSignal := endSignals[newMessage] == 1
	signalsReceived := len(endSignals)
	mutex.Unlock()

	log.Infof("End-Message #%d received.", signalsReceived)

	// Waiting for the total needed End-Signals to send the own End-Message.
	if (signalsReceived == aggregator.endSignals) && newSignal {
		log.Infof("All End-Messages were received.")
		wg.Done()
	}
}

func (aggregator *Aggregator) sendAggregatedData(aggregatedData rabbitmq.UserData, wg *sync.WaitGroup) {
	data, err := json.Marshal(aggregatedData)
	if err != nil {
		log.Errorf("Error generating Json from (%s). Err: '%s'", aggregatedData, err)
	} else {
		aggregator.outputQueue1.PublishData(data)
		aggregator.outputQueue2.PublishData(data)
	}
	wg.Done()
}

func (aggregator *Aggregator) Stop() {
	log.Infof("Closing User Aggregator connections.")
	aggregator.connection.Close()
	aggregator.channel.Close()
}
