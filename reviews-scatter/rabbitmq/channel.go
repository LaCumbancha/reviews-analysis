package rabbitmq

import (
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"
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
		log.Fatalf("Error creating queue %s", queue.name)
	}
}

func (queue *RabbitQueue) Publish(reviewId string, fullReview string) {
	err := queue.channel.Publish(
		"",     							// Exchange
		queue.name, 						// Routing Key
		false,  							// Mandatory
		false,  							// Immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(fullReview),
		})

	if err != nil {
		log.Errorf("Error sending review %s to queue %s", reviewId, queue.name, err)
	}

	log.Debugf("Review %s sent.", reviewId)
}
