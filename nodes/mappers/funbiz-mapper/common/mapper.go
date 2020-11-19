package common

import (
	"fmt"
	"sync"
	"encoding/json"

	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/mappers/funbiz-mapper/utils"
	"github.com/LaCumbancha/reviews-analysis/nodes/mappers/funbiz-mapper/rabbitmq"
)

type MapperConfig struct {
	RabbitIp			string
	RabbitPort			string
	InputQueueName		string
	OutputQueueName		string
	FunnyFilters 		int
}

type Mapper struct {
	connection 		*amqp.Connection
	channel 		*amqp.Channel
	inputFanout 	*rabbitmq.RabbitInputFanout
	outputQueue 	*rabbitmq.RabbitOutputQueue
}

func NewMapper(config MapperConfig) *Mapper {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%s/", config.RabbitIp, config.RabbitPort))
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ at (%s, %s). Err: '%s'", config.RabbitIp, config.RabbitPort , err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a RabbitMQ channel. Err: '%s'", err)
	}

	inputFanout := rabbitmq.NewRabbitInputFanout(config.InputQueueName, ch)
	outputQueue := rabbitmq.NewRabbitOutputQueue(config.OutputQueueName, config.FunnyFilters, ch)
	mapper := &Mapper {
		connection:		conn,
		channel:		ch,
		inputFanout:	inputFanout,
		outputQueue:	outputQueue,
	}

	return mapper
}

func (mapper *Mapper) Run() {
	log.Infof("Starting to listen for reviews.")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for message := range mapper.inputFanout.ConsumeReviews() {
			messageBody := string(message.Body)

			if messageBody == rabbitmq.END_MESSAGE {
				log.Infof("End-Message received.")
				mapper.outputQueue.PublishFinish()
				//rabbitmq.AckMessage(&message, rabbitmq.END_MESSAGE)
				wg.Done()
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
}

func (mapper *Mapper) processReview(rawReview string) {
	var fullReview FullReview
	json.Unmarshal([]byte(rawReview), &fullReview)

	mappedReview := &FunnyBusinessData {
		BusinessId:		fullReview.BusinessId,
		Funny:			fullReview.Funny,
	}

	data, err := json.Marshal(mappedReview)
	if err != nil {
		log.Errorf("Error generating Json from (%s). Err: '%s'", mappedReview, err)
	}

	mapper.outputQueue.PublishData(data)
}

func (mapper *Mapper) Stop() {
	log.Infof("Closing Funbiz-Mapper connections.")
	mapper.connection.Close()
	mapper.channel.Close()
}
