package core

import (
	"fmt"
	"time"
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
	WeekdayAggregators 	int
	ReviewsInputs 		int
}

type Mapper struct {
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	inputDirect 		*rabbit.RabbitInputDirect
	outputDirect 		*rabbit.RabbitOutputDirect
	outputPartitions	map[string]string
	endSignals			int
}

func NewMapper(config MapperConfig) *Mapper {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputDirect := rabbit.NewRabbitInputDirect(channel, props.ReviewsScatterOutput, props.WeekdayMapperTopic, props.WeekdayMapperInput)
	outputDirect := rabbit.NewRabbitOutputDirect(channel, props.WeekdayMapperOutput, comms.EndMessage(config.Instance))

	mapper := &Mapper {
		connection:			connection,
		channel:			channel,
		inputDirect:		inputDirect,
		outputDirect:		outputDirect,
		outputPartitions:	utils.GeneratePartitionMap(config.WeekdayAggregators, PartitionableValues),
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

func (mapper *Mapper) mapData(bulkNumber int, rawReviewsBulk string) []comms.WeekdayData {
	var review comms.FullReview
	var weekdayDataList []comms.WeekdayData

	rawReviews := strings.Split(rawReviewsBulk, "\n")
	for _, rawReview := range rawReviews {
		json.Unmarshal([]byte(rawReview), &review)

		if rawReview != "" {
			reviewDate, err := time.Parse("2006-01-02", review.Date[0:10])

			if err != nil {
				log.Errorf("Error parsing date from review %s (given date: %s). Err: %s", review.ReviewId, review.Date, err)
			} else {
				reviewWeekday := reviewDate.Weekday()
				mappedReview := comms.WeekdayData {
					Weekday:	reviewWeekday.String(),
				}

				weekdayDataList = append(weekdayDataList, mappedReview)
			}	
		} else {
			log.Warnf("Empty RawReview.")
		}	
	}

	return weekdayDataList
}

func (mapper *Mapper) sendMappedData(bulkNumber int, mappedBulk []comms.WeekdayData, wg *sync.WaitGroup) {
	dataListByPartition := make(map[string][]comms.WeekdayData)

	for _, data := range mappedBulk {
		partition := mapper.outputPartitions[data.Weekday]

		if partition != "" {
			weekdayDataListPartitioned := dataListByPartition[partition]

			if weekdayDataListPartitioned != nil {
				dataListByPartition[partition] = append(weekdayDataListPartitioned, data)
			} else {
				dataListByPartition[partition] = append(make([]comms.WeekdayData, 0), data)
			}

		} else {
			log.Errorf("Couldn't calculate partition for weekday '%s'.", data.Weekday)
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
	log.Infof("Closing Weekday Mapper connections.")
	mapper.connection.Close()
	mapper.channel.Close()
}
