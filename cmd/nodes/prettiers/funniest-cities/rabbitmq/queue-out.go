package rabbitmq

import (
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"
)

type RabbitOutputQueue struct {
	channel 			*amqp.Channel
	name 				string

}

func NewRabbitOutputQueue(name string, channel *amqp.Channel) *RabbitOutputQueue {
	queue := &RabbitOutputQueue {
		channel: 		channel,
		name:			name,
	}

	queue.initialize()
	return queue
}

func (queue *RabbitOutputQueue) initialize() {
	_, err := queue.channel.QueueDeclare(
		queue.name, 						// Name
		false,   							// Durable
		false,   							// Auto-Deleted
		false,   							// Exclusive
		false,   							// No-wait
		nil,     							// Args
	)

	if err != nil {
		log.Fatalf("Error creating queue %s. Err: '%s'", queue.name, err)
	} else {
		log.Debugf("Queue %s created.", queue.name)
	}
}

func (queue *RabbitOutputQueue) PublishData(data string) {
	err := queue.channel.Publish(
		"",     							// Exchange
		queue.name, 						// Routing Key
		false,  							// Mandatory
		false,  							// Immediate
		amqp.Publishing{
			ContentType: 	"text/plain",
			Body:        	[]byte(data),
		})

	if err != nil {
		log.Errorf("Error sending results data to queue %s. Err: '%s'", queue.name, err)
	} else {
		log.Debugf("Results data sent to queue %s.", queue.name)
	}
}

func (queue *RabbitOutputQueue) PublishFinish() {
	err := queue.channel.Publish(
		"", 							// Exchange
		queue.name,     				// Routing Key
		false,  						// Mandatory
		false,  						// Immediate
		amqp.Publishing{
			ContentType: 	"text/plain",
			Body:        	[]byte(END_MESSAGE),
		},
	)

	if err != nil {
		log.Errorf("Error sending End-Message to queue %s. Err: '%s'", queue.name, err)
	} else {
		log.Infof("End-Message sent to queue %s.", queue.name)
	}
}
