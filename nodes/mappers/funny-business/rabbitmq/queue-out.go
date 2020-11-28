package rabbitmq

import (
	"fmt"
	"encoding/json"
	"github.com/streadway/amqp"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/nodes/mappers/funny-business/logger"
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

func (queue *RabbitOutputQueue) PublishData(bulkNumber int, funbizDataList []FunnyBusinessData) {
	data, err := json.Marshal(funbizDataList)
	if err != nil {
		log.Errorf("Error generating Json from mapped bulk #%d. Err: '%s'", bulkNumber, err)
	} else {
		err := queue.channel.Publish(
			"",     							// Exchange
			queue.name, 						// Routing Key
			false,  							// Mandatory
			false,  							// Immediate
			amqp.Publishing{
				ContentType: 	"text/plain",
				Body:        	data,
			},
		)

		if err != nil {
			log.Errorf("Error sending mapped bulk #%d to queue %s. Err: '%s'", bulkNumber, queue.name, err)
		} else {
			logb.Instance().Infof(fmt.Sprintf("Mapped bulk #%d sent to queue %s.", bulkNumber, queue.name), bulkNumber)
		}
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
