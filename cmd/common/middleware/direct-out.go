package middleware

import (
	"github.com/streadway/amqp"
	"github.com/LaCumbancha/reviews-analysis/cmd/common/utils"
	
	log "github.com/sirupsen/logrus"
)

type RabbitOutputDirect struct {
	instance		string
	channel 		*amqp.Channel
	exchange 		string
	partitionsMap	map[string]string
	finishMessage	string
}

func NewRabbitOutputDirect(channel *amqp.Channel, name string, instance string, endMessage string, partitionsMap map[string]string) *RabbitOutputDirect {
	rabbitDirect := &RabbitOutputDirect {
		instance:			instance,
		channel: 			channel,
		exchange:			name,
		partitionsMap:		partitionsMap,
		finishMessage:		endMessage + instance,
	}

	rabbitDirect.initialize()
	return rabbitDirect
}

func (direct *RabbitOutputDirect) initialize() {
	err := direct.channel.ExchangeDeclare(
	  	direct.exchange,   						// Name
	  	"direct", 								// Type
	  	false,     								// Durable
	  	false,    								// Auto-Deleted
	  	false,    								// Internal
	  	false,    								// No-Wait
	  	nil,      								// Arguments
	)

	if err != nil {
		log.Fatalf("Error creating direct-exchange %s. Err: '%s'", direct.exchange, err)
	} else {
		log.Infof("Direct-Exchange %s created.", direct.exchange)
	}
}

func (direct *RabbitOutputDirect) PublishData(data []byte, partition string) error {
	return direct.channel.Publish(
		direct.exchange, 						// Exchange
		partition,    							// Routing Key
		false,  								// Mandatory
		false,  								// Immediate
		amqp.Publishing{
		    ContentType: 	"text/plain",
		    Body:        	data,
		},
	)
}

func (direct *RabbitOutputDirect) PublishFinish() {
	partitions := utils.GetMapDistinctValues(direct.partitionsMap)

	for _, partition := range partitions {
		err := direct.channel.Publish(
  			direct.exchange, 					// Exchange
  			partition,     						// Routing Key
  			false,  							// Mandatory
  			false,  							// Immediate
  			amqp.Publishing{
  			    ContentType: 	"text/plain",
  			    Body:        	[]byte(direct.finishMessage),
  			},
  		)

		if err != nil {
			log.Errorf("Error sending End-Message to direct-exchange %s (partition %s). Err: '%s'", direct.exchange, partition, err)
		} else {
			log.Infof("End-Message sent to direct-exchange %s (partition %s).", direct.exchange, partition)
		}	
	}
}
