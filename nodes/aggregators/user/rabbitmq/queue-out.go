package rabbitmq

import (
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"
)

type RabbitOutputQueue struct {
	instance		string
	channel 		*amqp.Channel
	name 			string
	endSignals 		int
}

func NewRabbitOutputQueue(name string, instance string, endSignals int, channel *amqp.Channel) *RabbitOutputQueue {
	queue := &RabbitOutputQueue {
		instance:		instance,
		channel: 		channel,
		name:			name,
		endSignals:		RETRIES * endSignals,
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

func (queue *RabbitOutputQueue) PublishData(data []byte) {
	err := queue.channel.Publish(
		"",     							// Exchange
		queue.name, 						// Routing Key
		false,  							// Mandatory
		false,  							// Immediate
		amqp.Publishing{
			ContentType: 	"text/plain",
			Body:        	data,
		})

	if err != nil {
		log.Errorf("Error sending weekday data (%s) to queue %s. Err: '%s'", data, queue.name, err)
	} else {
		log.Debugf("User reviews aggregated data (%s) sent to queue %s.", data, queue.name)
	}
}

func (queue *RabbitOutputQueue) PublishFinish() {
	for idx := 1; idx <= queue.endSignals; idx++ {
		err := queue.channel.Publish(
  			"", 							// Exchange
	  		queue.name,     				// Routing Key
	  		false,  						// Mandatory
	  		false,  						// Immediate
	  		amqp.Publishing{
	  		    ContentType: 	"text/plain",
	  		    Body:        	[]byte(END_MESSAGE + queue.instance),
	  		},
	  	)

		if err != nil {
			log.Errorf("Error sending End-Message #%d to queue %s. Err: '%s'", idx, queue.name, err)
		} else {
			log.Infof("End-Message #%d sent to queue %s.", idx, queue.name)
		}
	}
}
