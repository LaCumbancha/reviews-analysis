package common

import (
	"fmt"
	"sync"
	"encoding/json"
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/mappers/funny-business/utils"
	"github.com/LaCumbancha/reviews-analysis/nodes/mappers/funny-business/rabbitmq"
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
	inputDirect 	*rabbitmq.RabbitInputDirect
	outputQueue 	*rabbitmq.RabbitOutputQueue
	endSignals		int
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

	inputDirect := rabbitmq.NewRabbitInputDirect(rabbitmq.INPUT_EXCHANGE_NAME, ch)
	outputQueue := rabbitmq.NewRabbitOutputQueue(rabbitmq.OUTPUT_QUEUE_NAME, config.Instance, config.FunbizFilters, ch)
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
		for message := range mapper.inputDirect.ConsumeReviews() {
			messageBody := string(message.Body)

			if rabbitmq.IsEndMessage(messageBody) {
				mapper.processEndSignal(messageBody, endSignals, endSignalsMutex, &wg)
				//rabbitmq.AckMessage(&message, rabbitmq.END_MESSAGE)
			} else {
				review := messageBody
				log.Infof("Review %s received.", utils.GetReviewId(review))

				wg.Add(1)
				go func() {
					mapper.processReview(review)
					//rabbitmq.AckMessage(&message, utils.GetReviewId(review))
					wg.Done()
				}()
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

	log.Infof("End-Message #%d received.", signalsReceived)

	// Waiting for the total needed End-Signals to send the own End-Message.
	if (signalsReceived == mapper.endSignals) && newSignal {
		log.Infof("All End-Messages were received.")
		wg.Done()
	}
}

func (mapper *Mapper) processReview(rawReview string) {
	var fullReview rabbitmq.FullReview
	json.Unmarshal([]byte(rawReview), &fullReview)

	mappedReview := &rabbitmq.FunnyBusinessData {
		BusinessId:		fullReview.BusinessId,
		Funny:			fullReview.Funny,
	}

	data, err := json.Marshal(mappedReview)
	if err != nil {
		log.Errorf("Error generating Json from (%s). Err: '%s'", mappedReview, err)
	} else {
		mapper.outputQueue.PublishData(data)
	}
}

func (mapper *Mapper) Stop() {
	log.Infof("Closing Funny-Business Mapper connections.")
	mapper.connection.Close()
	mapper.channel.Close()
}
