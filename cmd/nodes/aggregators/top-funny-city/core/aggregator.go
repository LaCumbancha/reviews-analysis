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
	FuncitAggregators	int
	TopSize				int
}

type Aggregator struct {
	connection 		*amqp.Connection
	channel 		*amqp.Channel
	calculator		*Calculator
	inputQueue 		*rabbit.RabbitInputQueue
	outputQueue 	*rabbit.RabbitOutputQueue
	endSignals		int
}

func NewAggregator(config AggregatorConfig) *Aggregator {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputQueue := rabbit.NewRabbitInputQueue(channel, props.FuncitAggregatorOutput)
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.FuncitTopOutput, comms.EndMessage(config.Instance), comms.EndSignals(1))

	aggregator := &Aggregator {
		connection:		connection,
		channel:		channel,
		calculator:		NewCalculator(config.TopSize),
		inputQueue:		inputQueue,
		outputQueue:	outputQueue,
		endSignals:		config.FuncitAggregators,
	}

	return aggregator
}

func (aggregator *Aggregator) Run() {
	log.Infof("Starting to listen for funny-city data.")

	var distinctEndSignals = make(map[string]int)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		bulkCounter := 0
		for message := range aggregator.inputQueue.ConsumeData() {
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
				logb.Instance().Infof(fmt.Sprintf("Funcit data bulk #%d received.", bulkCounter), bulkCounter)

				wg.Add(1)
				go func(bulkNumber int, bulk string) {
					aggregator.calculator.Save(bulkNumber, bulk)
					wg.Done()
				}(bulkCounter, messageBody)
			}
		}
	}()
	
    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are received.
    wg.Wait()

    cityCounter := 0
    for _, cityData := range aggregator.calculator.RetrieveTopTen() {
    	cityCounter++

    	wg.Add(1)
    	go aggregator.sendTopTenData(cityCounter, cityData, &wg)
	}

    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are sent.
    wg.Wait()

    // Sending End-Message to consumers.
    aggregator.outputQueue.PublishFinish()
}

func (aggregator *Aggregator) sendTopTenData(cityNumber int, topTenCity comms.FunnyCityData, wg *sync.WaitGroup) {
	data, err := json.Marshal(topTenCity)
	if err != nil {
		log.Errorf("Error generating Json from funniest city #%d data. Err: '%s'", cityNumber, err)
	} else {
		err := aggregator.outputQueue.PublishData(data)

		if err != nil {
			log.Errorf("Error sending funniest city #%d data to output queue %s. Err: '%s'", cityNumber, aggregator.outputQueue.Name, err)
		} else {
			log.Infof("Funniest city #%d data sent to output queue %s.", cityNumber, aggregator.outputQueue.Name)
		}
	}

	wg.Done()
}

func (aggregator *Aggregator) Stop() {
	log.Infof("Closing Funny-City Aggregator connections.")
	aggregator.connection.Close()
	aggregator.channel.Close()
}
