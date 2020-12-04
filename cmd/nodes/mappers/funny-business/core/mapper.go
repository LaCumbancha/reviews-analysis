package core

import (
	"fmt"
	"sync"
	"strings"
	"encoding/json"
	"github.com/streadway/amqp"

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
	ReviewsInputs		int
	FunbizFilters 		int
}

type Mapper struct {
	connection 		*amqp.Connection
	channel 		*amqp.Channel
	inputDirect 	*rabbit.RabbitInputDirect
	outputQueue 	*rabbit.RabbitOutputQueue
	endSignals		int
}

func NewMapper(config MapperConfig) *Mapper {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputDirect := rabbit.NewRabbitInputDirect(channel, props.ReviewsScatterOutput, props.FunbizMapperTopic, props.FunbizMapperInput)
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.FunbizMapperOutput, comms.EndMessage(config.Instance), comms.EndSignals(config.FunbizFilters))

	mapper := &Mapper {
		connection:		connection,
		channel:		channel,
		inputDirect:	inputDirect,
		outputQueue:	outputQueue,
		endSignals:		config.ReviewsInputs,
	}

	return mapper
}

func (mapper *Mapper) Run() {
	log.Infof("Starting to listen for reviews.")

	var endSignalsMutex = &sync.Mutex{}
	var endSignals = make(map[string]int)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		bulkCounter := 0
		for message := range mapper.inputDirect.ConsumeData() {
			messageBody := string(message.Body)

			if comms.IsEndMessage(messageBody) {
				mapper.processEndSignal(messageBody, endSignals, endSignalsMutex, &wg)
			} else {
				bulkCounter++
				logb.Instance().Infof(fmt.Sprintf("Review bulk #%d received.", bulkCounter), bulkCounter)

				wg.Add(1)
				go func(bulkNumber int, bulk string) {
					mappedBulk := mapper.mapData(bulkNumber, bulk)
					mapper.sendMappedData(bulkNumber, mappedBulk, &wg)
				}(bulkCounter, messageBody)
			}
		}
	}()
	
    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are sent.
    wg.Wait()

    // Publishing end messages.
    mapper.outputQueue.PublishFinish()
}

func (mapper *Mapper) processEndSignal(newMessage string, endSignals map[string]int, mutex *sync.Mutex, wg *sync.WaitGroup) {
	mutex.Lock()
	endSignals[newMessage] = endSignals[newMessage] + 1
	newSignal := endSignals[newMessage] == 1
	signalsReceived := len(endSignals)
	mutex.Unlock()

	if newSignal {
		log.Infof("End-Message #%d received.", signalsReceived)
	}

	// Waiting for the total needed End-Signals to send the own End-Message.
	if (signalsReceived == mapper.endSignals) && newSignal {
		log.Infof("All End-Messages were received.")
		wg.Done()
	}
}

func (mapper *Mapper) mapData(bulkNumber int, rawReviewsBulk string) []comms.FunnyBusinessData {
	var review comms.FullReview
	var funbizDataList []comms.FunnyBusinessData

	rawReviews := strings.Split(rawReviewsBulk, "\n")
	for _, rawReview := range rawReviews {
		if rawReview != "" {
			json.Unmarshal([]byte(rawReview), &review)

			mappedReview := comms.FunnyBusinessData {
				BusinessId:		review.BusinessId,
				Funny:			review.Funny,
			}

			funbizDataList = append(funbizDataList, mappedReview)
		} else {
			log.Warnf("Empty RawReview.")
		}
	}

	return funbizDataList
}

func (mapper *Mapper) sendMappedData(bulkNumber int, mappedBulk []comms.FunnyBusinessData, wg *sync.WaitGroup) {
	data, err := json.Marshal(mappedBulk)
	if err != nil {
		log.Errorf("Error generating Json from mapped bulk #%d. Err: '%s'", bulkNumber, err)
	} else {
		err := mapper.outputQueue.PublishData(data)

		if err != nil {
			log.Errorf("Error sending mapped bulk #%d to output queue %s. Err: '%s'", bulkNumber, mapper.outputQueue.Name, err)
		} else {
			logb.Instance().Infof(fmt.Sprintf("Mapped bulk #%d sent to output queue %s.", bulkNumber, mapper.outputQueue.Name), bulkNumber)
		}
	}

	wg.Done()
}

func (mapper *Mapper) Stop() {
	log.Infof("Closing Funny-Business Mapper connections.")
	mapper.connection.Close()
	mapper.channel.Close()
}
