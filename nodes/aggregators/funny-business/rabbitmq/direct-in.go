package rabbitmq

import (
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"
)

type RabbitInputDirect struct {
	channel 			*amqp.Channel
	exchange 			string
	queue 				string
}

func NewRabbitInputDirect(name string, inputTopic string, channel *amqp.Channel) *RabbitInputDirect {
	direct := &RabbitInputDirect {
		channel: 	channel,
		exchange:	name,
	}

	direct.initialize(inputTopic)
	return direct
}

func (direct *RabbitInputDirect) initialize(inputTopic string) {
	err := direct.channel.ExchangeDeclare(
		direct.exchange, 		// Name
		"direct",				// Type
		false,   				// Durable
		false,   				// Auto-Deleted
		false,   				// Internal
		false,   				// No-Wait
		nil,     				// Args
	)

	if err != nil {
		log.Fatalf("Error creating direct-exchange %s. Err: '%s'", direct.exchange, err)
	} else {
		log.Debugf("Fanout-Exchange %s created.", direct.exchange)
	}

	queue, err := direct.channel.QueueDeclare(
        "",  					// Name
        false, 					// Durable
        false, 					// Auto-Deleted
        false,  				// Exclusive
        false, 					// No-Wait
        nil,   					// Args
    )

    if err != nil {
		log.Fatalf("Error creating queue for direct-exchange %s. Err: '%s'", direct.exchange, err)
	} else {
		log.Debugf("Queue %s for direct-exchange %s created.", queue.Name, direct.exchange)
	}

	err = direct.channel.QueueBind(
        queue.Name, 			// Queue
        inputTopic,  	 		// Routing-Key
        direct.exchange, 		// Exchange
        false,
        nil,
    )

    if err != nil {
		log.Fatalf("Error binding queue %s to direct-exchange %s. Err: '%s'", queue.Name, direct.exchange, err)
	} else {
		log.Debugf("Queue %s binded to direct-exchange %s.", queue.Name, direct.exchange)
	}

	direct.queue = queue.Name
}

func (direct *RabbitInputDirect) ConsumeData() <-chan amqp.Delivery {
	data, err := direct.channel.Consume(
		direct.queue, 			// Name
		"",     				// Consumer
		true,   				// Auto-ACK
		false,  				// Exclusive
		false,  				// No-Local
		false,  				// No-Wait
		nil,    				// Args
	)

	if err != nil {
		log.Errorf("Error receiving funny-business data from direct-exchange %s (through queue %s). Err: '%s'", direct.exchange, direct.queue, err)
	}

	return data
}
