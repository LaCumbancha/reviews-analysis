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
}

type Mapper struct {
	connection 		*amqp.Connection
	channel 		*amqp.Channel
	inputQueue 		*rabbitmq.RabbitQueue
	outputQueue 	*rabbitmq.RabbitQueue
}

type FullReview struct {
	ReviewId 		string 						`json:"review_id",omitempty`
	UserId 			string 						`json:"user_id",omitempty`
	BusinessId 		string 						`json:"business_id",omitempty`
	Stars 			int8 						`json:"stars",omitempty`
	Useful			int8 						`json:"useful",omitempty`
	Funny 			int8 						`json:"funny",omitempty`
	Cool			int8 						`json:"cool",omitempty`
	Text 			string 						`json:"text",omitempty`
	Date 			string 						`json:"date",omitempty`
}

type FunnyBusinessData struct {
	BusinessId 		string 						`json:"business_id",omitempty`
	Funny 			int8 						`json:"funny",omitempty`
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

	inputQueue := rabbitmq.NewRabbitQueue(config.InputQueueName, ch)
	outputQueue := rabbitmq.NewRabbitQueue(config.OutputQueueName, ch)
	mapper := &Mapper {
		connection:		conn,
		channel:		ch,
		inputQueue:		inputQueue,
		outputQueue:	outputQueue,
	}

	return mapper
}

func (mapper *Mapper) Run() {
	log.Infof("Starting to listen for reviews.")

	var wg sync.WaitGroup
	for message := range mapper.inputQueue.ConsumeReviews() {
		go func() {
			wg.Add(1)
			review := string(message.Body)
			log.Infof("Review %s received.", utils.GetReviewId(review))
			mapper.processReview(review)
			wg.Done()
		}()
	}

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
	mapper.connection.Close()
	mapper.channel.Close()
}
