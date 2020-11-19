package rabbitmq

import (
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"
)

type RabbitOutputFanout struct {
	channel 		*amqp.Channel
	exchange 		string
}

func NewRabbitOutputFanout(name string, channel *amqp.Channel) *RabbitOutputFanout {
	rabbitFanout := &RabbitOutputFanout {
		channel: 	channel,
		exchange:	name,
	}

	rabbitFanout.initialize()
	return rabbitFanout
}

func (fanout *RabbitOutputFanout) initialize() {
	err := fanout.channel.ExchangeDeclare(
	  	fanout.exchange,   	// Name
	  	"fanout", 			// Type
	  	false,     			// Durable
	  	false,    			// Auto-Deleted
	  	false,    			// Internal
	  	false,    			// No-Wait
	  	nil,      			// Arguments
	)

	if err != nil {
		log.Fatalf("Error creating fanout-exchange %s. Err: '%s'", fanout.exchange, err)
	} else {
		log.Infof("Fanout-Exchange %s created.", fanout.exchange)
	}
}

func (fanout *RabbitOutputFanout) PublishReview(reviewId string, review string) {
	err := fanout.channel.Publish(
  		fanout.exchange, 					// Exchange
  		"",     							// Routing Key
  		false,  							// Mandatory
  		false,  							// Immediate
  		amqp.Publishing{
  		    ContentType: 	"text/plain",
  		    Body:        	[]byte(review),
  		},
  	)

	if err != nil {
		log.Errorf("Error sending message %s to fanout-exchange %s. Err: '%s'", reviewId, fanout.exchange, err)
	} else {
		log.Infof("Message %s sent to fanout-exchange %s.", reviewId, fanout.exchange)
	}	
}

func (fanout *RabbitOutputFanout) PublishFinish() {
	err := fanout.channel.Publish(
  		fanout.exchange, 					// Exchange
  		"",     							// Routing Key
  		false,  							// Mandatory
  		false,  							// Immediate
  		amqp.Publishing{
  		    ContentType: 	"text/plain",
  		    Body:        	[]byte(END_MESSAGE),
  		},
  	)

	if err != nil {
		log.Errorf("Error sending End-Message to fanout-exchange %s. Err: '%s'", fanout.exchange, err)
	} else {
		log.Infof("End-Message sent to fanout-exchange %s.", fanout.exchange)
	}	
}
