package rabbitmq

import (
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"
)

type RabbitInputFanout struct {
	channel 			*amqp.Channel
	exchange 			string
	queue 				string
}

func NewRabbitInputFanout(name string, channel *amqp.Channel) *RabbitInputFanout {
	fanout := &RabbitInputFanout {
		channel: 	channel,
		exchange:	name,
	}

	fanout.initialize()
	return fanout
}

func (fanout *RabbitInputFanout) initialize() {
	err := fanout.channel.ExchangeDeclare(
		fanout.exchange, 	// Name
		"fanout",			// Type
		false,   			// Durable
		false,   			// Auto-Deleted
		false,   			// Internal
		false,   			// No-Wait
		nil,     			// Args
	)

	if err != nil {
		log.Fatalf("Error creating exchange %s. Err: '%s'", fanout.exchange, err)
	} else {
		log.Debugf("Exchange %s created.", fanout.exchange)
	}

	queue, err := fanout.channel.QueueDeclare(
        "",    				// Name
        false, 				// Durable
        false, 				// Auto-Deleted
        true,  				// Exclusive
        false, 				// No-Wait
        nil,   				// Args
    )

    if err != nil {
		log.Fatalf("Error creating anonymous queue for exchange %s. Err: '%s'", fanout.exchange, err)
	} else {
		log.Debugf("Anonymous queue %s for exchange %s created.", queue.Name, fanout.exchange)
	}

	err = fanout.channel.QueueBind(
        queue.Name, 		// Queue
        "",     			// Routing-Key
        fanout.exchange, 	// Exchange
        false,
        nil,
    )

    if err != nil {
		log.Fatalf("Error binding anonymous queue %s to exchange %s. Err: '%s'", queue.Name, fanout.exchange, err)
	} else {
		log.Debugf("Anonymous queue %s binded to exchange %s.", queue.Name, fanout.exchange)
	}

	fanout.queue = queue.Name
}

func (fanout *RabbitInputFanout) ConsumeReviews() <-chan amqp.Delivery {
	reviews, err := fanout.channel.Consume(
		fanout.queue, 		// Name
		"",     			// Consumer
		false,   			// Auto-ACK
		false,  			// Exclusive
		false,  			// No-Local
		false,  			// No-Wait
		nil,    			// Args
	)

	if err != nil {
		log.Errorf("Error receiving reviews from exchange %s (through queue %s). Err: '%s'", fanout.exchange, fanout.queue, err)
	}

	return reviews
}
