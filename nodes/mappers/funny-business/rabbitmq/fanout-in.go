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
		log.Fatalf("Error creating fanout-exchange %s. Err: '%s'", fanout.exchange, err)
	} else {
		log.Debugf("Fanout-Exchange %s created.", fanout.exchange)
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
		log.Fatalf("Error creating anonymous queue for fanout-exchange %s. Err: '%s'", fanout.exchange, err)
	} else {
		log.Debugf("Anonymous queue %s for fanout-exchange %s created.", queue.Name, fanout.exchange)
	}

	err = fanout.channel.QueueBind(
        queue.Name, 		// Queue
        "",     			// Routing-Key
        fanout.exchange, 	// Exchange
        false,
        nil,
    )

    if err != nil {
		log.Fatalf("Error binding anonymous queue %s to fanout-exchange %s. Err: '%s'", queue.Name, fanout.exchange, err)
	} else {
		log.Debugf("Anonymous queue %s binded to fanout-exchange %s.", queue.Name, fanout.exchange)
	}

	fanout.queue = queue.Name
}

func (fanout *RabbitInputFanout) ConsumeReviews() <-chan amqp.Delivery {
	reviews, err := fanout.channel.Consume(
		fanout.queue, 		// Name
		"",     			// Consumer
		true,   			// Auto-ACK
		false,  			// Exclusive
		false,  			// No-Local
		false,  			// No-Wait
		nil,    			// Args
	)

	if err != nil {
		log.Errorf("Error receiving reviews from fanout-exchange %s (through queue %s). Err: '%s'", fanout.exchange, fanout.queue, err)
	}

	return reviews
}
