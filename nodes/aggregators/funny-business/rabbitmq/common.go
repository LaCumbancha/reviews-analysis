package rabbitmq

import (
	"strconv"
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"
)

func AckMessage(message *amqp.Delivery, messageId string) {
	if err := message.Ack(false); err != nil {
		log.Errorf("Error sending message %s ACK. Err: '%s'", message.MessageId, err)
	} else {
		log.Tracef("Sending message %s ACK.", messageId)
	}
}

func GeneratePartitionMap(partitions int) map[string]string {
	partitionsMap := make(map[string]string)

	for idx, value := range PARTITIONER_VALUES {
		partitionsMap[value] = strconv.Itoa(idx % partitions)
	}

	return partitionsMap
}
