package core

import (
	"fmt"
	"sync"
	"github.com/streadway/amqp"
	"github.com/LaCumbancha/reviews-analysis/cmd/nodes/aggregators/distinct-hash/rabbitmq"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	props "github.com/LaCumbancha/reviews-analysis/cmd/common/properties"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
	rabbit "github.com/LaCumbancha/reviews-analysis/cmd/common/middleware"
)

type AggregatorConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	InputTopic			string
	HashAggregators		int
	DishashFilters		int
	OutputBulkSize		int
}

type Aggregator struct {
	connection 		*amqp.Connection
	channel 		*amqp.Channel
	calculator		*Calculator
	inputDirect 	*rabbit.RabbitInputDirect
	outputQueue 	*rabbitmq.RabbitOutputQueue
	endSignals		int
}

func NewAggregator(config AggregatorConfig) *Aggregator {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputDirect := rabbit.NewRabbitInputDirect(channel, props.HashAggregatorOutput, config.InputTopic, "")
	outputQueue := rabbitmq.NewRabbitOutputQueue(props.DishashAggregatorOutput, config.Instance, config.DishashFilters, channel)

	aggregator := &Aggregator {
		connection:		connection,
		channel:		channel,
		calculator:		NewCalculator(config.OutputBulkSize),
		inputDirect:	inputDirect,
		outputQueue:	outputQueue,
		endSignals:		config.HashAggregators,
	}

	return aggregator
}

func (aggregator *Aggregator) Run() {
	log.Infof("Starting to listen for distinct hashed-texts.")

	var endSignalsMutex = &sync.Mutex{}
	var endSignals = make(map[string]int)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		bulkCounter := 0
		for message := range aggregator.inputDirect.ConsumeData() {
			messageBody := string(message.Body)

			if comms.IsEndMessage(messageBody) {
				aggregator.processEndSignal(messageBody, endSignals, endSignalsMutex, &wg)
			} else {
				bulkCounter++
				logb.Instance().Infof(fmt.Sprintf("Distinct hashed texts data bulk #%d received.", bulkCounter), bulkCounter)

				wg.Add(1)
				go func(bulkNumber int, bulk string) {
					aggregator.calculator.Aggregate(bulkNumber, bulk)
					wg.Done()
				}(bulkCounter, messageBody)
			}
		}
	}()
	
    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are received.
    wg.Wait()

    outputBulkNumber := 0
    for _, aggregatedData := range aggregator.calculator.RetrieveData() {
		outputBulkNumber++
    	logb.Instance().Infof(fmt.Sprintf("Aggregated bulk #%d generated.", outputBulkNumber), outputBulkNumber)

		wg.Add(1)
		go func(bulkNumber int, aggregatedBulk []comms.DishashData) {
			aggregator.outputQueue.PublishData(bulkNumber, aggregatedBulk)
			wg.Done()
		}(outputBulkNumber, aggregatedData)
	}

    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are sent.
    wg.Wait()

    // Sending End-Message to consumers.
    aggregator.outputQueue.PublishFinish()
}

func (aggregator *Aggregator) processEndSignal(newMessage string, endSignals map[string]int, mutex *sync.Mutex, wg *sync.WaitGroup) {
	mutex.Lock()
	endSignals[newMessage] = endSignals[newMessage] + 1
	newSignal := endSignals[newMessage] == 1
	signalsReceived := len(endSignals)
	mutex.Unlock()

	if newSignal {
		log.Infof("End-Message #%d received.", signalsReceived)
	}

	// Waiting for the total needed End-Signals to send the own End-Message.
	if (signalsReceived == aggregator.endSignals) && newSignal {
		log.Infof("All End-Messages were received.")
		wg.Done()
	}
}

func (aggregator *Aggregator) Stop() {
	log.Infof("Closing Distinct-Hash Aggregator connections.")
	aggregator.connection.Close()
	aggregator.channel.Close()
}
