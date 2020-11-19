package main

import (
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		conn, err = amqp.Dial("amqp://guest:guest@localhost:5672/")
		failOnError(err, "Failed to connect to RabbitMQ")
	}
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"FunnyBusinessMapper", 		// Name
		false,   					// Durable
		false,   					// Auto-Delete
		false,   					// Exclusive
		false,   					// No-Wait
		nil,     					// Args
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, 					// Queue
		"",     					// Consumer
		true,   					// Auto-ACK
		false,  					// Exclusive
		false,  					// No-Local
		false,  					// No-Wait
		nil,    					// Args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
