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
	RabbitIp			string
	RabbitPort			string
	FunbizFilters 		int
}

type Mapper struct {
	connection 		*amqp.Connection
	channel 		*amqp.Channel
	inputDirect 	*rabbitmq.RabbitInputDirect
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

	inputDirect := rabbitmq.NewRabbitInputDirect(rabbitmq.INPUT_EXCHANGE_NAME, ch)
	outputQueue := rabbitmq.NewRabbitOutputQueue(rabbitmq.OUTPUT_QUEUE_NAME, config.FunbizFilters, ch)
	mapper := &Mapper {
		connection:		conn,
		channel:		ch,
		inputDirect:	inputDirect,
		outputQueue:	outputQueue,
	}

	return mapper
}

func (mapper *Mapper) Run() {
	log.Infof("Starting to listen for reviews.")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for message := range mapper.inputDirect.ConsumeReviews() {
			messageBody := string(message.Body)

			if messageBody == rabbitmq.END_MESSAGE {
				log.Infof("End-Message received.")

				// Using WaitGroup to avoid publishing finish message before all others are sent.
				wg.Done()
				wg.Wait()

				mapper.outputQueue.PublishFinish()
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
	}

	mapper.outputQueue.PublishData(data)
}

func (mapper *Mapper) Stop() {
	log.Infof("Closing Funny-Business Mapper connections.")
	mapper.connection.Close()
	mapper.channel.Close()
}
