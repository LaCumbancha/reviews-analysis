package middleware

import (
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"
)

type RabbitInputDirect struct {
	Exchange 			string
	channel 			*amqp.Channel
	queue 				string
}

func NewRabbitInputDirect(channel *amqp.Channel, name string, inputTopic string, queue string) *RabbitInputDirect {
	direct := &RabbitInputDirect {
		Exchange:	name,
		channel: 	channel,
		queue:		queue,
	}

	direct.initialize(inputTopic)
	return direct
}

func (direct *RabbitInputDirect) initialize(inputTopic string) {
	err := direct.channel.ExchangeDeclare(
		direct.Exchange, 		// Name
		"direct",				// Type
		false,   				// Durable
		false,   				// Auto-Deleted
		false,   				// Internal
		false,   				// No-Wait
		nil,     				// Args
	)

	if err != nil {
		log.Fatalf("Error creating direct-exchange %s. Err: '%s'", direct.Exchange, err)
	} else {
		log.Infof("Direct-Exchange %s created.", direct.Exchange)
	}

	queue, err := direct.channel.QueueDeclare(
        direct.queue,  			// Name
        false, 					// Durable
        false, 					// Auto-Deleted
        false,  				// Exclusive
        false, 					// No-Wait
        nil,   					// Args
    )

    if err != nil {
		log.Fatalf("Error creating queue for direct-exchange %s. Err: '%s'", direct.Exchange, err)
	} else {
		log.Infof("Queue %s for direct-exchange %s created.", queue.Name, direct.Exchange)
	}

	err = direct.channel.QueueBind(
        queue.Name, 			// Queue
        inputTopic,  	 		// Routing-Key
        direct.Exchange, 		// Exchange
        false,
        nil,
    )

    if err != nil {
		log.Fatalf("Error binding queue %s to direct-exchange %s. Err: '%s'", queue.Name, direct.Exchange, err)
	} else {
		log.Infof("Queue %s binded to direct-exchange %s.", queue.Name, direct.Exchange)
	}

	direct.queue = queue.Name
}

func (direct *RabbitInputDirect) ConsumeData() (<-chan amqp.Delivery, error) {
	data, err := direct.channel.Consume(
		direct.queue, 			// Name
		"",     				// Consumer
		true,   				// Auto-ACK
		false,  				// Exclusive
		false,  				// No-Local
		false,  				// No-Wait
		nil,    				// Args
	)

	return data, err
}
