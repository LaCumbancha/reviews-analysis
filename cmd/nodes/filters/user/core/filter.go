package core

import (
	"fmt"
	"sync"
	"encoding/json"
	"github.com/streadway/amqp"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	utils "github.com/LaCumbancha/reviews-analysis/cmd/common/utils"
	props "github.com/LaCumbancha/reviews-analysis/cmd/common/properties"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
	rabbit "github.com/LaCumbancha/reviews-analysis/cmd/common/middleware"
)

type FilterConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	MinReviews			int
	UserAggregators		int
	StarsJoiners		int
}

type Filter struct {
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	minReviews 			int
	inputQueue 			*rabbit.RabbitInputQueue
	outputQueue 		*rabbit.RabbitOutputQueue
	outputDirect 		*rabbit.RabbitOutputDirect
	outputPartitions	map[string]string
	endSignals			int
}

func NewFilter(config FilterConfig) *Filter {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputQueue := rabbit.NewRabbitInputQueue(channel, props.UserAggregatorOutput)
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.UserFilterOutput, comms.EndMessage(config.Instance), comms.EndSignals(1))
	outputDirect := rabbit.NewRabbitOutputDirect(channel, props.BestUsersFilterOutput, comms.EndMessage(config.Instance))

	filter := &Filter {
		connection:			connection,
		channel:			channel,
		minReviews:			config.MinReviews,
		inputQueue:			inputQueue,
		outputQueue:		outputQueue,
		outputDirect:		outputDirect,
		outputPartitions:	utils.GeneratePartitionMap(config.StarsJoiners, PartitionableValues),
		endSignals:			config.UserAggregators,
	}

	return filter
}

func (filter *Filter) Run() {
	log.Infof("Starting to listen for user reviews data.")

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
				logb.Instance().Infof(fmt.Sprintf("User data bulk #%d received.", bulkCounter), bulkCounter)

				wg.Add(1)
				go func(bulkNumber int, bulk string) {
					filteredData := filter.filterData(bulkNumber, bulk)
					filter.sendFilteredData(bulkNumber, filteredData, &wg)
				}(bulkCounter, messageBody)
			}
		}
	}()
	
    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are sent.
    wg.Wait()

    // Publishing end messages.
    filter.outputQueue.PublishFinish()

    for _, partition := range utils.GetMapDistinctValues(filter.outputPartitions) {
    	filter.outputDirect.PublishFinish(partition)
    }
}

func (filter *Filter) filterData(bulkNumber int, rawUserDataBulk string) []comms.UserData {
	var userDataList []comms.UserData
	var filteredUserDataList []comms.UserData
	json.Unmarshal([]byte(rawUserDataBulk), &userDataList)

	for _, userData := range userDataList {
		if (userData.Reviews >= filter.minReviews) {
			filteredUserDataList = append(filteredUserDataList, userData)
		}
	}

	return filteredUserDataList
}

func (filter *Filter) sendFilteredData(bulkNumber int, filteredBulk []comms.UserData, wg *sync.WaitGroup) {
	data, err := json.Marshal(filteredBulk)
	if err != nil {
		log.Errorf("Error generating Json from filtered bulk #%d. Err: '%s'", bulkNumber, err)
	} else {
		err := filter.outputQueue.PublishData(data)

		if err != nil {
			log.Errorf("Error sending filtered bulk #%d to output queue %s. Err: '%s'", bulkNumber, filter.outputQueue.Name, err)
		} else {
			logb.Instance().Infof(fmt.Sprintf("Filtered bulk #%d sent to output queue %s.", bulkNumber, filter.outputQueue.Name), bulkNumber)
		}
	}

	dataListByPartition := make(map[string][]comms.UserData)

	for _, data := range filteredBulk {
		partition := filter.outputPartitions[string(data.UserId[0])]

		if partition != "" {
			userDataListPartitioned := dataListByPartition[partition]

			if userDataListPartitioned != nil {
				dataListByPartition[partition] = append(userDataListPartitioned, data)
			} else {
				dataListByPartition[partition] = append(make([]comms.UserData, 0), data)
			}

		} else {
			log.Errorf("Couldn't calculate partition for user '%s'.", data.UserId)
		}
	}

	for partition, userDataListPartitioned := range dataListByPartition {
		outputData, err := json.Marshal(userDataListPartitioned)

		if err != nil {
			log.Errorf("Error generating Json from (%s). Err: '%s'", userDataListPartitioned, err)
		} else {

			err := filter.outputDirect.PublishData(outputData, partition)

			if err != nil {
				log.Errorf("Error sending bulk #%d to direct-exchange %s (partition %s). Err: '%s'", bulkNumber, filter.outputDirect.Exchange, partition, err)
			} else {
				logb.Instance().Infof(fmt.Sprintf("Bulk #%d sent to direct-exchange %s (partition %s).", bulkNumber, filter.outputDirect.Exchange, partition), bulkNumber)
			}	
		}
	}

	wg.Done()
}

func (filter *Filter) Stop() {
	log.Infof("Closing User Filter connections.")
	filter.connection.Close()
	filter.channel.Close()
}
