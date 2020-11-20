package receivers

import (
    "github.com/spf13/viper"
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"
)

func Direct(configEnv *viper.Viper, conn *amqp.Connection, ch *amqp.Channel) {
    defer conn.Close()
    defer ch.Close()

    exchange := configEnv.GetString("exchange")
    if exchange == "" {
        log.Fatalf("Exchange variable missing")
    }

    topic := configEnv.GetString("topic")
    if topic == "" {
        log.Fatalf("Topic variable missing")
    }

	err := ch.ExchangeDeclare(
        exchange, 	             // name
        "direct",      			// type
        false,          		// durable
        false,         			// auto-deleted
        false,         			// internal
        false,         			// no-wait
        nil,           			// arguments
    )
    if err != nil {
        log.Fatalf("Failed to declare direct-exchange %s. Err: %s", exchange, err)
    }

    q, err := ch.QueueDeclare(
        "",    					// name
        false, 					// durable
        false, 					// delete when unused
        true,  					// exclusive
        false, 					// no-wait
        nil,   					// arguments
    )
    if err != nil {
        log.Fatalf("Failed to declare direct-exchange %s anonymous queue. Err: %s", exchange, err)
    }

    err = ch.QueueBind(
        q.Name,        			// queue name
        topic,             		// routing key
        exchange, 	            // exchange
        false,
        nil,
    )
    if err != nil {
        log.Fatalf("Failed to bind direct-exchange %s with anonymous queue %s. Err: %s", exchange, q.Name, err)
    }

    msgs, err := ch.Consume(
        q.Name, // queue
        "",     // consumer
        true,   // auto ack
        false,  // exclusive
        false,  // no local
        false,  // no wait
        nil,    // args
    )
    if err != nil {
        log.Fatalf("Failed to register a consumer at direct-exchange %s (queue %s). Err: %s", exchange, q.Name, err)
    }

	forever := make(chan bool)
    log.Infof("Starting listening at direct-exchange %s (queue %s, topic %s).", exchange, q.Name, topic)

	go func() {
		for d := range msgs {
			log.Printf(string(d.Body))
		}
	}()

	<-forever
}
