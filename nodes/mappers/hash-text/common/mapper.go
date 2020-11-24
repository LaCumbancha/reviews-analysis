package common

import (
	"fmt"
	"sync"
	"crypto/md5"
	"encoding/json"
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/mappers/hash-text/utils"
	"github.com/LaCumbancha/reviews-analysis/nodes/mappers/hash-text/rabbitmq"
)

type MapperConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	ReviewsInputs		int
	HashAggregators		int
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

	inputDirect := rabbitmq.NewRabbitInputDirect(rabbitmq.INPUT_EXCHANGE_NAME, config.Instance, ch)
	outputDirect := rabbitmq.NewRabbitOutputDirect(rabbitmq.OUTPUT_EXCHANGE_NAME, config.Instance, config.HashAggregators, ch)
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
		for message := range mapper.inputDirect.ConsumeReviews() {
			messageBody := string(message.Body)

			if rabbitmq.IsEndMessage(messageBody) {
				mapper.processEndSignal(messageBody, endSignals, endSignalsMutex, &wg)
			} else {
				review := messageBody
				log.Infof("Review %s received.", utils.GetReviewId(review))

				wg.Add(1)
				go func() {
					mapper.processReview(review)
					wg.Done()
				}()
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

	log.Infof("End-Message #%d received.", signalsReceived)

	// Waiting for the total needed End-Signals to send the own End-Message.
	if (signalsReceived == mapper.endSignals) && newSignal {
		log.Infof("All End-Messages were received.")
		mapper.inputDirect.Close()
		wg.Done()
	}
}

func (mapper *Mapper) processReview(rawReview string) {
	var fullReview rabbitmq.FullReview
	json.Unmarshal([]byte(rawReview), &fullReview)

	hasher := md5.New()
	hasher.Write([]byte(fullReview.Text))
	hashedText := fmt.Sprintf("%x", hasher.Sum(nil))

	mappedReview := &rabbitmq.HashedTextData {
		UserId:			fullReview.UserId,
		HashedText:		hashedText,
	}

	data, err := json.Marshal(mappedReview)
	if err != nil {
		log.Errorf("Error generating Json from (%s). Err: '%s'", mappedReview, err)
	} else {
		mapper.outputDirect.PublishData(data, fullReview.UserId)
	}
}

func (mapper *Mapper) Stop() {
	log.Infof("Closing User Mapper connections.")
	mapper.channel.Close()
	mapper.connection.Close()
}
