package core

import (
	"fmt"
	"sync"
	"strings"
	"crypto/md5"
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
	ReviewsInputs		int
	HashAggregators		int
}

type Mapper struct {
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	inputDirect 		*rabbit.RabbitInputDirect
	outputDirect 		*rabbit.RabbitOutputDirect
	outputPartitions	map[string]string
	endSignals 			int
}

func NewMapper(config MapperConfig) *Mapper {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputDirect := rabbit.NewRabbitInputDirect(channel, props.ReviewsScatterOutput, props.HashMapperTopic, props.HashMapperInput)
	outputDirect := rabbit.NewRabbitOutputDirect(channel, props.HashMapperOutput, comms.EndMessage(config.Instance))

	mapper := &Mapper {
		connection:			connection,
		channel:			channel,
		inputDirect:		inputDirect,
		outputDirect:		outputDirect,
		outputPartitions:	utils.GeneratePartitionMap(config.HashAggregators, PartitionableValues),
		endSignals:			config.ReviewsInputs,
	}

	return mapper
}

func (mapper *Mapper) Run() {
	log.Infof("Starting to listen for reviews.")

	var distinctEndSignals = make(map[string]int)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		bulkCounter := 0
		for message := range mapper.inputDirect.ConsumeData() {
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
				logb.Instance().Infof(fmt.Sprintf("Review bulk #%d received.", bulkCounter), bulkCounter)

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

func (mapper *Mapper) mapData(bulkNumber int, rawReviewsBulk string) []comms.HashedTextData {
	var review comms.FullReview
	var hashTextDataList []comms.HashedTextData

	rawReviews := strings.Split(rawReviewsBulk, "\n")
	for _, rawReview := range rawReviews {
		json.Unmarshal([]byte(rawReview), &review)

		if (review.UserId != "") {
			hasher := md5.New()
			hasher.Write([]byte(review.Text))
			hashedText := fmt.Sprintf("%x", hasher.Sum(nil))
	
			mappedReview := comms.HashedTextData {
				UserId:			review.UserId,
				HashedText:		hashedText,
			}

			hashTextDataList = append(hashTextDataList, mappedReview)			
		} else {
			log.Warnf("Empty UserID detected in raw review %s.", rawReview)
		}
	}

	return hashTextDataList
}

func (mapper *Mapper) sendMappedData(bulkNumber int, mappedBulk []comms.HashedTextData, wg *sync.WaitGroup) {
	dataListByPartition := make(map[string][]comms.HashedTextData)

	for _, data := range mappedBulk {
		partition := mapper.outputPartitions[string(data.UserId[0])]

		if partition != "" {
			hashedDataList := dataListByPartition[partition]

			if hashedDataList != nil {
				dataListByPartition[partition] = append(hashedDataList, data)
			} else {
				dataListByPartition[partition] = append(make([]comms.HashedTextData, 0), data)
			}

		} else {
			log.Errorf("Couldn't calculate partition for user (%s).", data.UserId)
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
	log.Infof("Closing User Mapper connections.")
	mapper.connection.Close()
	mapper.channel.Close()
}
