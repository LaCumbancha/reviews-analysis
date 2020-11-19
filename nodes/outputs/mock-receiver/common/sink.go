package common

import (
	"fmt"
	"sync"

	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/outputs/sink/rabbitmq"
)

type SinkConfig struct {
	Data							string
	RabbitIp						string
	RabbitPort						string
	FunnyCityQueueName				string
	WeekdayHistogramQueueName		string
	TopUsersQueueName				string
	BestUsersQueueName				string
	BotUsersQueueName				string
}

type Sink struct {
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	funnyCityQueue 		*rabbitmq.RabbitInputQueue
}

func NewSink(config SinkConfig) *Sink {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%s/", config.RabbitIp, config.RabbitPort))
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ at (%s, %s). Err: '%s'", config.RabbitIp, config.RabbitPort , err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a RabbitMQ channel. Err: '%s'", err)
	}

	funnyCityQueue := rabbitmq.NewRabbitInputQueue(config.FunnyCityQueueName, ch)
	sink := &Sink {
		connection:			conn,
		channel:			ch,
		funnyCityQueue:		funnyCityQueue,
	}

	return sink
}

func (sink *Sink) Run() {
	log.Infof("Starting to listen for results.")

	var wg sync.WaitGroup
	// TODO: Should initialize WaitGroup in 5 when the service is fully operational.
	wg.Add(1)
	go func() {
		// TODO: Use this same logic for this and the other queues.
		//
		// for message := range sink.funnyCityQueue.ConsumeData() {
		// 	log.Infof("Top 10 Funniest Cities: %s", string(message.Body))
		// 	rabbitmq.AckMessage(&message, "FUNNIEST-CITIES")
		// 	wg.Done()
		// }

		for d := range sink.funnyCityQueue.ConsumeData() {
			log.Infof("Received data: %s", d.Body)
		}
	}()

    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are received.
    wg.Wait()
}

func (sink *Sink) Stop() {
	log.Infof("Closing Sink connections.")
	sink.connection.Close()
	sink.channel.Close()
}
