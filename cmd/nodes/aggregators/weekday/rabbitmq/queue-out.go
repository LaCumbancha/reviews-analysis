package rabbitmq

import (
	"encoding/json"
	"github.com/streadway/amqp"

	log "github.com/sirupsen/logrus"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

type RabbitOutputQueue struct {
	instance		string
	channel 		*amqp.Channel
	name 			string
}

func NewRabbitOutputQueue(name string, instance string, channel *amqp.Channel) *RabbitOutputQueue {
	queue := &RabbitOutputQueue {
		instance:		instance,
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

func (queue *RabbitOutputQueue) PublishData(aggregatedData comms.WeekdayData) {
	data, err := json.Marshal(aggregatedData)
	if err != nil {
		log.Errorf("Error generating Json from (%s). Err: '%s'", aggregatedData, err)
	} else {
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
			log.Errorf("Error sending %s aggregated reviews to queue %s. Err: '%s'", aggregatedData.Weekday, queue.name, err)
		} else {
			log.Debugf("%s aggregated reviews sent to queue %s.", aggregatedData.Weekday, queue.name)
		}
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
			Body:        	[]byte(comms.END_MESSAGE + queue.instance),
		},
	)

	if err != nil {
		log.Errorf("Error sending End-Message to queue %s. Err: '%s'", queue.name, err)
	} else {
		log.Infof("End-Message sent to queue %s.", queue.name)
	}
}
