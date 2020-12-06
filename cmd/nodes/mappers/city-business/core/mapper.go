package core

import (
	"fmt"
	"sync"
	"strings"
	"encoding/json"
	"github.com/streadway/amqp"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	utils "github.com/LaCumbancha/reviews-analysis/cmd/common/utils"
	props "github.com/LaCumbancha/reviews-analysis/cmd/common/properties"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
	rabbit "github.com/LaCumbancha/reviews-analysis/cmd/common/middleware"
)

type MapperConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	BusinessesInputs	int
	FuncitJoiners 		int
}

type Mapper struct {
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	inputQueue 			*rabbit.RabbitInputQueue
	outputDirect 		*rabbit.RabbitOutputDirect
	outputPartitions	map[string]string
	endSignals			int
}

func NewMapper(config MapperConfig) *Mapper {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputQueue := rabbit.NewRabbitInputQueue(channel, props.BusinessesScatterOutput)
	outputDirect := rabbit.NewRabbitOutputDirect(channel, props.CitbizMapperOutput, comms.EndMessage(config.Instance))

	mapper := &Mapper {
		connection:			connection,
		channel:			channel,
		inputQueue:			inputQueue,
		outputDirect:		outputDirect,
		outputPartitions:	utils.GeneratePartitionMap(config.FuncitJoiners, PartitionableValues),
		endSignals:			config.BusinessesInputs,
	}

	return mapper
}

func (mapper *Mapper) Run() {
	log.Infof("Starting to listen for business.")

	var distinctEndSignals = make(map[string]int)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		bulkCounter := 0
		for message := range mapper.inputQueue.ConsumeData() {
			messageBody := string(message.Body)

			if comms.IsEndMessage(messageBody) {
				newFinishReceived, allFinishReceived := comms.LastEndMessage(messageBody, distinctEndSignals, mapper.endSignals)

				if newFinishReceived {
					log.Infof("End-Message #%d received.", len(distinctEndSignals))
				}

				if allFinishReceived {
					log.Infof("All End-Messages were received.")
					wg.Done()
				}

			} else {
				bulkCounter++
				logb.Instance().Infof(fmt.Sprintf("Business bulk #%d received.", bulkCounter), bulkCounter)

				wg.Add(1)
				go func(bulkNumber int, bulk string) {
					mappedData := mapper.mapData(bulkNumber, bulk)
					mapper.sendMappedData(bulkNumber, mappedData, &wg)
				}(bulkCounter, messageBody)
			}
		}
	}()
	
    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are sent.
    wg.Wait()

    // Publishing end messages.
    for _, partition := range utils.GetMapDistinctValues(mapper.outputPartitions) {
    	mapper.outputDirect.PublishFinish(partition)
    }
}

func (mapper *Mapper) mapData(bulkNumber int, rawBusinessesBulk string) []comms.CityBusinessData {
	var business comms.FullBusiness
	var citbizDataList []comms.CityBusinessData

	rawBusinesses := strings.Split(rawBusinessesBulk, "\n")
	for _, rawBusiness := range rawBusinesses {
		if rawBusiness != "" {
			json.Unmarshal([]byte(rawBusiness), &business)
			
			mappedBusiness := comms.CityBusinessData {
				BusinessId:		business.BusinessId,
				City:			fmt.Sprintf("%s (%s)", business.City, business.State),
			}

			citbizDataList = append(citbizDataList, mappedBusiness)
		} else {
			log.Warnf("Empty RawBusiness.")
		}
	}

	return citbizDataList
}

func (mapper *Mapper) sendMappedData(bulkNumber int, mappedBulk []comms.CityBusinessData, wg *sync.WaitGroup) {
	dataListByPartition := make(map[string][]comms.CityBusinessData)

	for _, data := range mappedBulk {
		partition := mapper.outputPartitions[string(data.BusinessId[0])]

		if partition != "" {
			citbizDataListPartitioned := dataListByPartition[partition]

			if citbizDataListPartitioned != nil {
				dataListByPartition[partition] = append(citbizDataListPartitioned, data)
			} else {
				dataListByPartition[partition] = append(make([]comms.CityBusinessData, 0), data)
			}

		} else {
			log.Errorf("Couldn't calculate partition for business '%s'.", data.BusinessId)
		}
	}

	for partition, userDataListPartitioned := range dataListByPartition {
		outputData, err := json.Marshal(userDataListPartitioned)

		if err != nil {
			log.Errorf("Error generating Json from (%s). Err: '%s'", userDataListPartitioned, err)
		} else {

			err := mapper.outputDirect.PublishData(outputData, partition)

			if err != nil {
				log.Errorf("Error sending bulk #%d to direct-exchange %s (partition %s). Err: '%s'", bulkNumber, mapper.outputDirect.Exchange, partition, err)
			} else {
				logb.Instance().Infof(fmt.Sprintf("Bulk #%d sent to direct-exchange %s (partition %s).", bulkNumber, mapper.outputDirect.Exchange, partition), bulkNumber)
			}	
		}
	}

	wg.Done()
}

func (mapper *Mapper) Stop() {
	log.Infof("Closing City-Business Mapper connections.")
	mapper.connection.Close()
	mapper.channel.Close()
}
