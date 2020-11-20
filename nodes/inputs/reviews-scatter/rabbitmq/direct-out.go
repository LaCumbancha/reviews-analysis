package rabbitmq

import (
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"
)

type RabbitOutputDirect struct {
	channel 		*amqp.Channel
	exchange 		string
	endSignalsMap 	map[string]int
}

func NewRabbitOutputDirect(name string, funbizSigs int, weekdaysSigs int, hashesSigs int, usersSigs int, starsSigs int, channel *amqp.Channel) *RabbitOutputDirect {
	rabbitDirect := &RabbitOutputDirect {
		channel: 			channel,
		exchange:			name,
		endSignalsMap:		GenerateSignalsMap(funbizSigs, weekdaysSigs, hashesSigs, usersSigs, starsSigs),
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

func (direct *RabbitOutputDirect) PublishReview(review string, reviewId string) {
	for _, partition := range PARTITIONER_VALUES {
		err := direct.channel.Publish(
	  		direct.exchange, 						// Exchange
	  		partition,    							// Routing Key
	  		false,  								// Mandatory
	  		false,  								// Immediate
	  		amqp.Publishing{
	  		    ContentType: 	"text/plain",
	  		    Body:        	[]byte(review),
	  		},
	  	)

		if err != nil {
			log.Errorf("Error sending review %s to direct-exchange %s (partition %s). Err: '%s'", reviewId, direct.exchange, partition, err)
		} else {
			log.Infof("Review %s sent to direct-exchange %s (partition %s).", reviewId, direct.exchange, partition)
		}	
	}
}

func (direct *RabbitOutputDirect) PublishFinish() {
	for _, partition := range PARTITIONER_VALUES {
		for idx := 0 ; idx < direct.endSignalsMap[partition]; idx++ {
			err := direct.channel.Publish(
	  			direct.exchange, 						// Exchange
	  			partition,    							// Routing Key
	  			false,  								// Mandatory
	  			false,  								// Immediate
	  			amqp.Publishing{
	  			    ContentType: 	"text/plain",
	  			    Body:        	[]byte(END_MESSAGE),
	  			},
	  		)

			if err != nil {
				log.Errorf("Error sending End-Message #%d to direct-exchange %s (partition %s). Err: '%s'", idx+1, direct.exchange, partition, err)
			} else {
				log.Infof("End-Message #%d sent to direct-exchange %s (partition %s).", idx+1, direct.exchange, partition)
			}
		}
	}
}
