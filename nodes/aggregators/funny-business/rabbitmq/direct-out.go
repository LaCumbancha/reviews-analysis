package rabbitmq

import (
	"fmt"
	"strconv"
	"encoding/json"
	"github.com/streadway/amqp"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/nodes/aggregators/funny-business/logger"
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

func (direct *RabbitOutputDirect) PublishData(bulkNumber int, funbizDataList []FunnyBusinessData) {
	dataListByPartition := make(map[string][]FunnyBusinessData)

	for _, data := range funbizDataList {
		partition := direct.partitionMap[string(data.BusinessId[0])]

		if partition != "" {
			funbizDataListPartitioned := dataListByPartition[partition]

			if funbizDataListPartitioned != nil {
				dataListByPartition[partition] = append(funbizDataListPartitioned, data)
			} else {
				dataListByPartition[partition] = append(make([]FunnyBusinessData, 0), data)
			}

		} else {
			log.Errorf("Couldn't calculate partition for business '%s'.", data.BusinessId)
		}
	}

	for partition, funbizDataListPartitioned := range dataListByPartition {
		outputData, err := json.Marshal(funbizDataListPartitioned)

		if err != nil {
			log.Errorf("Error generating Json from (%s). Err: '%s'", funbizDataListPartitioned, err)
		} else {

			err := direct.channel.Publish(
				direct.exchange, 						// Exchange
				partition,    							// Routing Key
				false,  								// Mandatory
				false,  								// Immediate
				amqp.Publishing{
				    ContentType: 	"text/plain",
				    Body:        	outputData,
				},
			)

			if err != nil {
				log.Errorf("Error sending bulk #%d to direct-exchange %s (partition %s). Err: '%s'", bulkNumber, direct.exchange, partition, err)
			} else {
				logb.Instance().Infof(fmt.Sprintf("Bulk #%d sent to direct-exchange %s (partition %s).", bulkNumber, direct.exchange, partition), bulkNumber)
			}	
		}
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
