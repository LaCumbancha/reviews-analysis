package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
)

type RabbitOutputQueue struct {
	channel 		*amqp.Channel
	name 			string
	endSignals 		int
}

func NewRabbitOutputQueue(name string, endSignals int, channel *amqp.Channel) *RabbitOutputQueue {
	queue := &RabbitOutputQueue {
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

func (queue *RabbitOutputQueue) PublishBulk(bulkNumber int, bulk string) {
	err := queue.channel.Publish(
		"",     							// Exchange
		queue.name, 						// Routing Key
		false,  							// Mandatory
		false,  							// Immediate
		amqp.Publishing{
			ContentType: 	"text/plain",
			Body:        	[]byte(bulk),
		})

	if err != nil {
		log.Errorf("Error sending bulk #%d to queue %s. Err: '%s'", bulkNumber, queue.name, err)
	} else {
		logb.Instance().Infof(fmt.Sprintf("Bulk #%d sent to queue %s.", bulkNumber, queue.name), bulkNumber)
	}
}

func (queue *RabbitOutputQueue) PublishFinish() {
	errors := false
	for idx := 1; idx <= queue.endSignals; idx++ {
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
			errors = true
			log.Errorf("Error sending End-Message #%d to queue %s. Err: '%s'", idx, queue.name, err)
		}
	}

	if !errors {
		log.Infof("End-Message sent to queue %s.", queue.name)
	}
}
