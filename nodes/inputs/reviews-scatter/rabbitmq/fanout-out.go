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

func (fanout *RabbitOutputFanout) initialize0() {
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
		log.Fatalf("Error creating exchange %s. Err: '%s'", fanout.exchange, err)
	} else {
		log.Fatalf("Exchange %s created.", fanout.exchange)
	}
}

func (fanout *RabbitOutputFanout) PublishReview0(reviewId string, review string) {
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
		log.Errorf("Error sending message %s to fanout %s. Err: '%s'", reviewId, fanout.exchange, err)
	} else {
		log.Infof("Message %s sent to fanout %s.", reviewId, fanout.exchange)
	}	
}

func (fanout *RabbitOutputFanout) PublishFinish0() {
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
		log.Errorf("Error sending End-Message to fanout %s. Err: '%s'", fanout.exchange, err)
	} else {
		log.Infof("End-Message sent to fanout %s.", fanout.exchange)
	}	
}





func (fanout *RabbitOutputFanout) initialize() {
	_, err := fanout.channel.QueueDeclare(
	  	fanout.exchange,   	// Name
	  	false,     			// Durable
	  	false,    			// Auto-Deleted
	  	false,    			// Exclusive
	  	false,    			// No-Wait
	  	nil,      			// Arguments
	)

	if err != nil {
		log.Fatalf("Error creating exchange %s. Err: '%s'", fanout.exchange, err)
	}
}

func (fanout *RabbitOutputFanout) PublishReview(reviewId string, review string) {
	err := fanout.channel.Publish(
		"",									// Exchange
  		fanout.exchange, 					// Routing Key
  		false,  							// Mandatory
  		false,  							// Immediate
  		amqp.Publishing{
  		    ContentType: 	"text/plain",
  		    Body:        	[]byte(review),
  		},
  	)

	if err != nil {
		log.Errorf("Error sending message %s to fanout %s. Err: '%s'", reviewId, fanout.exchange, err)
	} else {
		log.Infof("Message %s sent to fanout %s.", reviewId, fanout.exchange)
	}	
}

func (fanout *RabbitOutputFanout) PublishFinish() {
	err := fanout.channel.Publish(
		"",									// Exchange
  		fanout.exchange, 					// Routing Key
  		false,  							// Mandatory
  		false,  							// Immediate
  		amqp.Publishing{
  		    ContentType: 	"text/plain",
  		    Body:        	[]byte(END_MESSAGE),
  		},
  	)

	if err != nil {
		log.Errorf("Error sending End-Message to fanout %s. Err: '%s'", fanout.exchange, err)
	} else {
		log.Infof("End-Message sent to fanout %s.", fanout.exchange)
	}	
}
