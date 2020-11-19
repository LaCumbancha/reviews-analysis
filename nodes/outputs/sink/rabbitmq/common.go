package rabbitmq

import (
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"
)

func AckMessage(message *amqp.Delivery) {
	if err := message.Ack(false); err != nil {
		log.Errorf("Error sending ACK of message %s. Err: '%s'", message.MessageId, err)
	}
}
