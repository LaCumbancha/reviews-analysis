package core

import (
	"fmt"
	"sync"
	"strings"
	"encoding/json"
	"github.com/streadway/amqp"
	"github.com/LaCumbancha/reviews-analysis/cmd/nodes/mappers/stars/rabbitmq"
	
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
	StarsFilters		int
}

type Mapper struct {
	connection 		*amqp.Connection
	channel 		*amqp.Channel
	inputDirect 	*rabbit.RabbitInputDirect
	outputQueue 	*rabbitmq.RabbitOutputQueue
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

	inputDirect := rabbit.NewRabbitInputDirect(ch, props.ReviewsScatterOutput, props.StarsMapperTopic, props.StarsMapperInput)
	outputQueue := rabbitmq.NewRabbitOutputQueue(props.StarsMapperOutput, config.Instance, config.StarsFilters, ch)
	mapper := &Mapper {
		connection:		conn,
		channel:		ch,
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
		inputDirectChannel, err := mapper.inputDirect.ConsumeData()
		if err != nil {
			log.Fatalf("Error receiving data from direct-exchange %s. Err: '%s'", mapper.inputDirect.Exchange, err)
		}

		bulkCounter := 0
		for message := range inputDirectChannel {
			messageBody := string(message.Body)

			if comms.IsEndMessage(messageBody) {
				mapper.processEndSignal(messageBody, endSignals, endSignalsMutex, &wg)
			} else {
				bulkCounter++
				logb.Instance().Infof(fmt.Sprintf("Review bulk #%d received.", bulkCounter), bulkCounter)

				wg.Add(1)
				go func(bulkCounter int, bulk string) {
					mapper.processReviewsBulk(bulkCounter, bulk)
					wg.Done()
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

func (mapper *Mapper) processReviewsBulk(bulkNumber int, rawReviewsBulk string) {
	var review comms.FullReview
	var starsDataList []comms.StarsData

	rawReviews := strings.Split(rawReviewsBulk, "\n")
	for _, rawReview := range rawReviews {
		json.Unmarshal([]byte(rawReview), &review)
	
		mappedReview := comms.StarsData {
			UserId:		review.UserId,
			Stars:		review.Stars,
		}

		starsDataList = append(starsDataList, mappedReview)
	}

	mapper.outputQueue.PublishData(bulkNumber, starsDataList)
}

func (mapper *Mapper) Stop() {
	log.Infof("Closing Stars Mapper connections.")
	mapper.connection.Close()
	mapper.channel.Close()
}
