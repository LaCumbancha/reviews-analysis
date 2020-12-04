package core

import (
	"fmt"
	"time"
	"sync"
	"strings"
	"encoding/json"
	"github.com/streadway/amqp"
	"github.com/LaCumbancha/reviews-analysis/cmd/nodes/mappers/weekday/rabbitmq"

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
	WeekdayAggregators 	int
	ReviewsInputs 		int
}

type Mapper struct {
	connection 		*amqp.Connection
	channel 		*amqp.Channel
	inputDirect 	*rabbit.RabbitInputDirect
	outputDirect 	*rabbitmq.RabbitOutputDirect
	endSignals		int
}

func NewMapper(config MapperConfig) *Mapper {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputDirect := rabbit.NewRabbitInputDirect(channel, props.ReviewsScatterOutput, props.WeekdayMapperTopic, props.WeekdayMapperInput)
	outputDirect := rabbitmq.NewRabbitOutputDirect(props.WeekdayMapperOutput, config.Instance, config.WeekdayAggregators, channel)

	mapper := &Mapper {
		connection:		connection,
		channel:		channel,
		inputDirect:	inputDirect,
		outputDirect:	outputDirect,
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
					mapper.processReviewsBulk(bulkNumber, bulk)
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

func (mapper *Mapper) processReviewsBulk(bulkNumber int, rawReviewsBulk string) {
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

	mapper.outputDirect.PublishData(bulkNumber, weekdayDataList)
}

func (mapper *Mapper) Stop() {
	log.Infof("Closing Weekday Mapper connections.")
	mapper.connection.Close()
	mapper.channel.Close()
}
