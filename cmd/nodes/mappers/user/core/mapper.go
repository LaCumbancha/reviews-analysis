package core

import (
	"fmt"
	"sync"
	"strings"
	"encoding/json"
	"github.com/streadway/amqp"
	"github.com/LaCumbancha/reviews-analysis/cmd/nodes/mappers/user/rabbitmq"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	props "github.com/LaCumbancha/reviews-analysis/cmd/common/properties"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

type MapperConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	ReviewsInputs		int
	UserAggregators		int
}

type Mapper struct {
	connection 		*amqp.Connection
	channel 		*amqp.Channel
	inputDirect 	*rabbitmq.RabbitInputDirect
	outputDirect 	*rabbitmq.RabbitOutputDirect
	endSignals 		int
}

func NewMapper(config MapperConfig) *Mapper {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%s/", config.RabbitIp, config.RabbitPort))
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ at (%s, %s). Err: '%s'", config.RabbitIp, config.RabbitPort, err)
	} else {
		log.Infof("Connected to RabbitMQ at (%s, %s).", config.RabbitIp, config.RabbitPort)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a RabbitMQ channel. Err: '%s'", err)
	} else {
		log.Infof("RabbitMQ channel opened.")
	}

	inputDirect := rabbitmq.NewRabbitInputDirect(props.ReviewsScatterOutput, ch)
	outputDirect := rabbitmq.NewRabbitOutputDirect(props.UserMapperOutput, config.Instance, config.UserAggregators, ch)
	mapper := &Mapper {
		connection:		conn,
		channel:		ch,
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
		for message := range mapper.inputDirect.ConsumeReviews() {
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
	var userDataList []comms.UserData

	rawReviews := strings.Split(rawReviewsBulk, "\n")
	for _, rawReview := range rawReviews {
		json.Unmarshal([]byte(rawReview), &review)

		if (review.UserId != "") {
			mappedReview := comms.UserData {
				UserId:		review.UserId,
			}

			userDataList = append(userDataList, mappedReview)
		} else {
			log.Warnf("Empty UserID detected in raw review %s.", rawReview)
		}
	}

	mapper.outputDirect.PublishData(bulkNumber, userDataList)
}

func (mapper *Mapper) Stop() {
	log.Infof("Closing User Mapper connections.")
	mapper.connection.Close()
	mapper.channel.Close()
}
