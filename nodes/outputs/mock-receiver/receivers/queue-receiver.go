package receivers

import (
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"
)

func Queue(configEnv *viper.Viper, conn *amqp.Connection, ch *amqp.Channel) {
	defer conn.Close()
	defer ch.Close()

    queue := configEnv.GetString("queue")
    if queue == "" {
        log.Fatalf("Queue variable missing")
    }

	q, err := ch.QueueDeclare(
		queue, 						// Name
		false,   					// Durable
		false,   					// Auto-Delete
		false,   					// Exclusive
		false,   					// No-Wait
		nil,     					// Args
	)
	if err != nil {
        log.Fatalf("Failed to declare queue %s. Err: %s", queue, err)
    }

	msgs, err := ch.Consume(
		q.Name, 					// Queue
		"",     					// Consumer
		true,   					// Auto-ACK
		false,  					// Exclusive
		false,  					// No-Local
		false,  					// No-Wait
		nil,    					// Args
	)
	if err != nil {
        log.Fatalf("Failed to register a consumer at queue %s. Err: %s", queue, err)
    }

    log.Infof("Starting listening at queue %s.", queue)
	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf(string(d.Body))
		}
	}()	
	<-forever
}
