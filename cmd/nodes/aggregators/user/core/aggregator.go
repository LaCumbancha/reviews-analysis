package core

import (
	"fmt"
	"sync"
	"encoding/json"
	"github.com/streadway/amqp"
	
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
	UserMappers 		int
	UserFilters 		int
	BotUserFilters		int
	OutputBulkSize		int
}

type Aggregator struct {
	connection 		*amqp.Connection
	channel 		*amqp.Channel
	calculator		*Calculator
	inputDirect 	*rabbit.RabbitInputDirect
	outputQueue1 	*rabbit.RabbitOutputQueue
	outputQueue2	*rabbit.RabbitOutputQueue
	endSignals		int
}

func NewAggregator(config AggregatorConfig) *Aggregator {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputDirect := rabbit.NewRabbitInputDirect(channel, props.UserMapperOutput, config.InputTopic, "")
	outputQueue1 := rabbit.NewRabbitOutputQueue(channel, props.UserAggregatorOutput, comms.EndMessage(config.Instance), comms.EndSignals(config.UserFilters))
	outputQueue2 := rabbit.NewRabbitOutputQueue(channel, props.BotUsersAggregatorOutput, comms.EndMessage(config.Instance), comms.EndSignals(config.BotUserFilters))

	aggregator := &Aggregator {
		connection:		connection,
		channel:		channel,
		calculator:		NewCalculator(config.OutputBulkSize),
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
				logb.Instance().Infof(fmt.Sprintf("User data bulk #%d received.", bulkCounter), bulkCounter)

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

    outputBulkCounter := 0
    for _, aggregatedData := range aggregator.calculator.RetrieveData() {
		outputBulkCounter++
    	logb.Instance().Infof(fmt.Sprintf("Aggregated bulk #%d generated.", outputBulkCounter), outputBulkCounter)

		wg.Add(1)
		go aggregator.sendAggregatedData(outputBulkCounter, aggregatedData, &wg)
	}

    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are sent.
    wg.Wait()

    // Sending End-Message to consumers.
    aggregator.outputQueue1.PublishFinish()
    aggregator.outputQueue2.PublishFinish()
}

func (aggregator *Aggregator) sendAggregatedData(bulkNumber int, aggregatedBulk []comms.UserData, wg *sync.WaitGroup) {
	data, err := json.Marshal(aggregatedBulk)
	if err != nil {
		log.Errorf("Error generating Json from aggregated bulk #%d. Err: '%s'", bulkNumber, err)
	} else {
		err := aggregator.outputQueue1.PublishData(data)

		if err != nil {
			log.Errorf("Error sending aggregated bulk #%d to output queue %s. Err: '%s'", bulkNumber, aggregator.outputQueue1.Name, err)
		} else {
			logb.Instance().Infof(fmt.Sprintf("Aggregated bulk #%d sent to output queue %s.", bulkNumber, aggregator.outputQueue1.Name), bulkNumber)
		}

		err = aggregator.outputQueue2.PublishData(data)

		if err != nil {
			log.Errorf("Error sending aggregated bulk #%d to output queue %s. Err: '%s'", bulkNumber, aggregator.outputQueue2.Name, err)
		} else {
			logb.Instance().Infof(fmt.Sprintf("Aggregated bulk #%d sent to output queue %s.", bulkNumber, aggregator.outputQueue2.Name), bulkNumber)
		}
	}

	wg.Done()
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
	log.Infof("Closing User Aggregator connections.")
	aggregator.connection.Close()
	aggregator.channel.Close()
}
