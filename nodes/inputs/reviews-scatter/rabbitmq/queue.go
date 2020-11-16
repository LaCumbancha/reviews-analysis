package rabbitmq

import (
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/inputs/reviews-scatter/utils"
)

type RabbitQueue struct {
	channel 			*amqp.Channel
	name 				string
}

func NewRabbitQueue(name string, channel *amqp.Channel) *RabbitQueue {
	rabbitQueue := &RabbitQueue {
		channel: 	channel,
		name:		name,
	}

	rabbitQueue.initialize()

	return rabbitQueue
}

func (queue *RabbitQueue) initialize() {
	_, err := queue.channel.QueueDeclare(
		queue.name, 	// Name
		false,   		// Durable
		false,   		// Delete when unused
		false,   		// Exclusive
		false,   		// No-wait
		nil,     		// Arguments
	)

	if err != nil {
		log.Fatalf("Error creating queue %s. Err: '%s'", queue.name, err)
	}
}

func (queue *RabbitQueue) PublishReview(review string) {
	reviewId := utils.GetReviewId(review)
	err := queue.channel.Publish(
		"",     							// Exchange
		queue.name, 						// Routing Key
		false,  							// Mandatory
		false,  							// Immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(review),
		})

	if err != nil {
		log.Errorf("Error sending review %s to queue %s. Err: '%s'", reviewId, queue.name, err)
	}

	log.Debugf("Review %s sent.", reviewId)
}
