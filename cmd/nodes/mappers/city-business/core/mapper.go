package core

import (
	"fmt"
	"sync"
	"strings"
	"encoding/json"
	"github.com/streadway/amqp"
	"github.com/LaCumbancha/reviews-analysis/cmd/nodes/mappers/city-business/rabbitmq"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
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
	connection 		*amqp.Connection
	channel 		*amqp.Channel
	inputQueue 		*rabbit.RabbitInputQueue
	outputDirect 	*rabbitmq.RabbitOutputDirect
	endSignals		int
}

func NewMapper(config MapperConfig) *Mapper {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputQueue := rabbit.NewRabbitInputQueue(channel, props.BusinessesScatterOutput)
	outputDirect := rabbitmq.NewRabbitOutputDirect(props.CitbizMapperOutput, config.Instance, config.FuncitJoiners, channel)

	mapper := &Mapper {
		connection:		connection,
		channel:		channel,
		inputQueue:		inputQueue,
		outputDirect:	outputDirect,
		endSignals:		config.BusinessesInputs,
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
					mapper.mapData(bulkCounter, bulk)
					wg.Done()
				}(bulkCounter, messageBody)
			}
		}
	}()
	
    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are sent.
    wg.Wait()

    // Publishing end messages.
    mapper.outputDirect.PublishFinish()
}

func (mapper *Mapper) mapData(bulkNumber int, rawBusinessesBulk string) {
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

	mapper.outputDirect.PublishData(bulkNumber, citbizDataList)
}

func (mapper *Mapper) Stop() {
	log.Infof("Closing City-Business Mapper connections.")
	mapper.connection.Close()
	mapper.channel.Close()
}
