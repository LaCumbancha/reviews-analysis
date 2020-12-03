package rabbitmq

import (
	"fmt"
	"strconv"
	"encoding/json"
	"github.com/streadway/amqp"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
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

func (direct *RabbitOutputDirect) PublishData(bulkNumber int, userDataList []comms.UserData) {
	dataListByPartition := make(map[string][]comms.UserData)

	for _, data := range userDataList {
		partition := direct.partitionMap[string(data.UserId[0])]

		if partition != "" {
			userDataListPartitioned := dataListByPartition[partition]

			if userDataListPartitioned != nil {
				dataListByPartition[partition] = append(userDataListPartitioned, data)
			} else {
				dataListByPartition[partition] = append(make([]comms.UserData, 0), data)
			}

		} else {
			log.Errorf("Couldn't calculate partition for weekday (%s).", data.UserId)
		}
	}

	for partition, userDataListPartitioned := range dataListByPartition {
		outputData, err := json.Marshal(userDataListPartitioned)

		if err != nil {
			log.Errorf("Error generating Json from (%s). Err: '%s'", userDataListPartitioned, err)
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
  			    Body:        	[]byte(comms.END_MESSAGE + direct.instance),
  			},
  		)

		if err != nil {
			log.Errorf("Error sending End-Message to direct-exchange %s (partition %d). Err: '%s'", direct.exchange, idx, err)
		} else {
			log.Infof("End-Message sent to direct-exchange %s (partition %d).", direct.exchange, idx)
		}	
	}
}
