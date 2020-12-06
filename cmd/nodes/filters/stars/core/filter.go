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
	StarsMappers		int
	StarsAggregators	int
}

type Filter struct {
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	inputQueue 			*rabbit.RabbitInputQueue
	outputDirect 		*rabbit.RabbitOutputDirect
	outputPartitions	map[string]string
	endSignals			int
}

func NewFilter(config FilterConfig) *Filter {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputQueue := rabbit.NewRabbitInputQueue(channel, props.StarsMapperOutput)
	outputDirect := rabbit.NewRabbitOutputDirect(channel, props.StarsFilterOutput, comms.EndMessage(config.Instance))

	filter := &Filter {
		connection:			connection,
		channel:			channel,
		inputQueue:			inputQueue,
		outputDirect:		outputDirect,
		outputPartitions:	utils.GeneratePartitionMap(config.StarsAggregators, PartitionableValues),
		endSignals:			config.StarsMappers,
	}

	return filter
}

func (filter *Filter) Run() {
	log.Infof("Starting to listen for user stars data.")

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
					filteredData := filter.filterData(bulkNumber, bulk)
					filter.sendFilteredData(bulkNumber, filteredData, &wg)
				}(bulkCounter, messageBody)
			}
		}
	}()
	
    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are sent.
    wg.Wait()

    // Publishing end messages.
    for _, partition := range utils.GetMapDistinctValues(filter.outputPartitions) {
    	filter.outputDirect.PublishFinish(partition)
    }
}

func (filter *Filter) filterData(bulkNumber int, rawStarsDataBulk string) []comms.StarsData {
	var starsDataList []comms.StarsData
	var filteredStarsDataList []comms.StarsData
	json.Unmarshal([]byte(rawStarsDataBulk), &starsDataList)

	for _, starsData := range starsDataList {
		if (starsData.Stars == 5.0) {
			filteredStarsDataList = append(filteredStarsDataList, starsData)
		}
	}
	
	return filteredStarsDataList
}

func (filter *Filter) sendFilteredData(bulkNumber int, filteredBulk []comms.StarsData, wg *sync.WaitGroup) {
	dataListByPartition := make(map[string][]comms.StarsData)

	for _, data := range filteredBulk {
		partition := filter.outputPartitions[string(data.UserId[0])]

		if partition != "" {
			starsDataListPartitioned := dataListByPartition[partition]

			if starsDataListPartitioned != nil {
				dataListByPartition[partition] = append(starsDataListPartitioned, data)
			} else {
				dataListByPartition[partition] = append(make([]comms.StarsData, 0), data)
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
	log.Infof("Closing Stars Filter connections.")
	filter.connection.Close()
	filter.channel.Close()
}
