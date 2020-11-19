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
		log.Fatalf("Error creating queue %s. Err: '%s'", queue.name, err)
	}
}

func (queue *RabbitQueue) PublishData(data []byte) {
	err := queue.channel.Publish(
		"",     							// Exchange
		queue.name, 						// Routing Key
		false,  							// Mandatory
		false,  							// Immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        data,
		})

	if err != nil {
		log.Errorf("Error sending mapped data (%s) to queue %s. Err: '%s'", data, queue.name, err)
	}

	log.Debugf("Mapped data (%s) sent.", data)
}

func (queue *RabbitQueue) ConsumeReviews() <-chan amqp.Delivery {
	reviews, err := queue.channel.Consume(
		queue.name, 		// Name
		"",     			// Consumer
		false,   			// AutoACK
		false,  			// Exclusive
		false,  			// No-Local
		false,  			// No-Wait
		nil,    			// Args
	)

	if err != nil {
		log.Errorf("Error receiving reviews from queue %s. Err: '%s'", queue.name, err)
	}

	return reviews
}
