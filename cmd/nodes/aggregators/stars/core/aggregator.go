package core

import (
	"fmt"
	"sync"
	"github.com/streadway/amqp"
	"github.com/LaCumbancha/reviews-analysis/cmd/nodes/aggregators/stars/rabbitmq"

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
	StarsFilters		int
	StarsJoiners		int
	OutputBulkSize		int
}

type Aggregator struct {
	connection 		*amqp.Connection
	channel 		*amqp.Channel
	calculator		*Calculator
	inputDirect 	*rabbit.RabbitInputDirect
	outputDirect 	*rabbitmq.RabbitOutputDirect
	endSignals		int
}

func NewAggregator(config AggregatorConfig) *Aggregator {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputDirect := rabbit.NewRabbitInputDirect(channel, props.StarsFilterOutput, config.InputTopic, "")
	outputDirect := rabbitmq.NewRabbitOutputDirect(props.StarsAggregatorOutput, config.Instance, config.StarsJoiners, channel)

	aggregator := &Aggregator {
		connection:		connection,
		channel:		channel,
		calculator:		NewCalculator(config.OutputBulkSize),
		inputDirect:	inputDirect,
		outputDirect:	outputDirect,
		endSignals:		config.StarsFilters,
	}

	return aggregator
}

func (aggregator *Aggregator) Run() {
	log.Infof("Starting to listen for user stars data.")

	var distinctEndSignals = make(map[string]int)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		bulkCounter := 0
		for message := range aggregator.inputDirect.ConsumeData() {
			messageBody := string(message.Body)

			if comms.IsEndMessage(messageBody) {
				newFinishReceived, allFinishReceived := comms.LastEndMessage(messageBody, distinctEndSignals, aggregator.endSignals)

				if newFinishReceived {
					log.Infof("End-Message #%d received.", len(distinctEndSignals))
				}

				if allFinishReceived {
					log.Infof("All End-Messages were received.")
					wg.Done()
				}

			} else {
				bulkCounter++
				logb.Instance().Infof(fmt.Sprintf("Stars bulk #%d received.", bulkCounter), bulkCounter)

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
		go func(bulkNumber int, aggregatedBulk []comms.UserData) {
			aggregator.outputDirect.PublishData(bulkNumber, aggregatedBulk)
			wg.Done()
		}(outputBulkNumber, aggregatedData)
	}

    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are sent.
    wg.Wait()

    // Sending End-Message to consumers.
    aggregator.outputDirect.PublishFinish()
}

func (aggregator *Aggregator) Stop() {
	log.Infof("Closing Stars Aggregator connections.")
	aggregator.connection.Close()
	aggregator.channel.Close()
}
