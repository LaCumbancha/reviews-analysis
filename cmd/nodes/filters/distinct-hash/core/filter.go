package core

import (
	"fmt"
	"sync"
	"encoding/json"
	"github.com/streadway/amqp"
	"github.com/LaCumbancha/reviews-analysis/cmd/nodes/filters/distinct-hash/rabbitmq"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	props "github.com/LaCumbancha/reviews-analysis/cmd/common/properties"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
	rabbit "github.com/LaCumbancha/reviews-analysis/cmd/common/middleware"
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
	inputQueue 		*rabbit.RabbitInputQueue
	outputDirect	*rabbitmq.RabbitOutputDirect
	endSignals		int
}

func NewFilter(config FilterConfig) *Filter {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputQueue := rabbit.NewRabbitInputQueue(channel, props.DishashAggregatorOutput)
	outputDirect := rabbitmq.NewRabbitOutputDirect(props.DishashFilterOutput, config.Instance, config.DishashJoiners, channel)

	filter := &Filter {
		connection:		connection,
		channel:		channel,
		minReviews:		config.MinReviews,
		inputQueue:		inputQueue,
		outputDirect:	outputDirect,
		endSignals:		config.DishashAggregators,
	}

	return filter
}

func (filter *Filter) Run() {
	log.Infof("Starting to listen for users distinct text-hashes.")

	var distinctEndSignals = make(map[string]int)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		bulkCounter := 0
		for message := range filter.inputQueue.ConsumeData() {
			messageBody := string(message.Body)

			if comms.IsEndMessage(messageBody) {
				newFinishReceived, allFinishReceived := comms.LastEndMessage(messageBody, distinctEndSignals, filter.endSignals)

				if newFinishReceived {
					log.Infof("End-Message #%d received.", len(distinctEndSignals))
				}

				if allFinishReceived {
					log.Infof("All End-Messages were received.")
					wg.Done()
				}

			} else {
				bulkCounter++
				logb.Instance().Infof(fmt.Sprintf("Funbiz data bulk #%d received.", bulkCounter), bulkCounter)

				wg.Add(1)
				go func(bulkNumber int, bulk string) {
					filter.filterData(bulkNumber, bulk)
					wg.Done()
				}(bulkCounter, messageBody)
			}
		}
	}()
	
    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are sent.
    wg.Wait()

    // Publishing end messages.
    filter.outputDirect.PublishFinish()
}

func (filter *Filter) filterData(bulkNumber int, rawDishashDataBulk string) {
	var dishashDataList []comms.DishashData
	var filteredDishashesDataList []comms.DishashData
	json.Unmarshal([]byte(rawDishashDataBulk), &dishashDataList)

	for _, dishashData := range dishashDataList {
		if (dishashData.Distinct == 1) {
			filteredDishashesDataList = append(filteredDishashesDataList, dishashData)	
		}
	}

	filter.outputDirect.PublishData(bulkNumber, filteredDishashesDataList)
}

func (filter *Filter) Stop() {
	log.Infof("Closing Distinct-Hash Filter connections.")
	filter.connection.Close()
	filter.channel.Close()
}
