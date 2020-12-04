package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
)

type RabbitOutputDirect struct {
	instance		string
	channel 		*amqp.Channel
	exchange 		string
	endSignalsMap 	map[string]int
}

func NewRabbitOutputDirect(name string, instance string, funbizSigs int, weekdaysSigs int, hashesSigs int, usersSigs int, starsSigs int, channel *amqp.Channel) *RabbitOutputDirect {
	rabbitDirect := &RabbitOutputDirect {
		instance:			instance,
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

func (direct *RabbitOutputDirect) PublishBulk(bulkNumber int, bulkData string) {
	for _, partition := range PARTITIONER_VALUES {
		err := direct.channel.Publish(
	  		direct.exchange, 						// Exchange
	  		partition,    							// Routing Key
	  		false,  								// Mandatory
	  		false,  								// Immediate
	  		amqp.Publishing{
	  		    ContentType: 	"text/plain",
	  		    Body:        	[]byte(bulkData),
	  		},
	  	)

		if err != nil {
			log.Errorf("Error sending bulk #%d to direct-exchange %s (partition %s). Err: '%s'", bulkNumber, direct.exchange, partition, err)
		} else {
			logb.Instance().Infof(fmt.Sprintf("Bulk #%d sent to direct-exchange %s (partition %s).", bulkNumber, direct.exchange, partition), bulkNumber)
		}	
	}
}

func (direct *RabbitOutputDirect) PublishFinish() {
	errors := false
	for _, partition := range PARTITIONER_VALUES {
		for idx := 0 ; idx < direct.endSignalsMap[partition]; idx++ {
			err := direct.channel.Publish(
	  			direct.exchange, 						// Exchange
	  			partition,    							// Routing Key
	  			false,  								// Mandatory
	  			false,  								// Immediate
	  			amqp.Publishing{
	  			    ContentType: 	"text/plain",
	  			    Body:        	[]byte(comms.EndMessage + direct.instance),
	  			},
	  		)

			if err != nil {
				errors = true
				log.Errorf("Error sending End-Message #%d to direct-exchange %s (partition %s). Err: '%s'", idx+1, direct.exchange, partition, err)
			}
		}

		if !errors {
			log.Infof("End-Message sent to direct-exchange %s (partition %s).", direct.exchange, partition)
		}
	}
}
