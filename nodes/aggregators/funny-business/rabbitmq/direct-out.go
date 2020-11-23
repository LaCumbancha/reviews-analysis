package rabbitmq

import (
	"strconv"
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"
)

type RabbitOutputDirect struct {
	instance		string
	channel 		*amqp.Channel
	exchange 		string
	partitions 		int
	partitionMap	map[string]string
}

func NewRabbitOutputDirect(name string, instance string, partitions int, channel *amqp.Channel) *RabbitOutputDirect {
	rabbitDirect := &RabbitOutputDirect {
		instance:		instance,
		channel: 		channel,
		exchange:		name,
		partitions:		partitions,
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

	// Generate PartitionsMap to send each message to the correct aggregator.
	direct.partitionMap = GeneratePartitionMap(direct.partitions)
	log.Tracef("Partition map generated for direct-exchange %s: %s.", direct.exchange, direct.partitionMap)
}

func (direct *RabbitOutputDirect) PublishData(data []byte, businessId string) {
	partition := direct.partitionMap[string(businessId[0])]
	log.Debugf("Exchange %s partition calculated for %s: %s.", direct.exchange, businessId, partition)
	
	if partition != "" {
		err := direct.channel.Publish(
  			direct.exchange, 						// Exchange
  			partition,    							// Routing Key
  			false,  								// Mandatory
  			false,  								// Immediate
  			amqp.Publishing{
  			    ContentType: 	"text/plain",
  			    Body:        	data,
  			},
  		)

		if err != nil {
			log.Errorf("Error sending funny data from business %s to direct-exchange %s (partition %s). Err: '%s'", businessId, direct.exchange, partition, err)
		} else {
			log.Infof("Funny data from business %s sent to direct-exchange %s (partition %s).", businessId, direct.exchange, partition)
		}	
	} else {
		log.Errorf("Couldn't calculate partition for business %s", businessId)
	}
	
}

func (direct *RabbitOutputDirect) PublishFinish() {
	for idx := 0 ; idx < direct.partitions ; idx++ {
		err := direct.channel.Publish(
  			direct.exchange, 					// Exchange
  			strconv.Itoa(idx),     				// Routing Key
  			false,  							// Mandatory
  			false,  							// Immediate
  			amqp.Publishing{
  			    ContentType: 	"text/plain",
  			    Body:        	[]byte(END_MESSAGE + direct.instance),
  			},
  		)

		if err != nil {
			log.Errorf("Error sending End-Message to direct-exchange %s (partition %d). Err: '%s'", direct.exchange, idx, err)
		} else {
			log.Infof("End-Message sent to direct-exchange %s (partition %d).", direct.exchange, idx)
		}	
	}
}
