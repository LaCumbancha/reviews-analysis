package common

import (
	"fmt"
	"sync"

	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/outputs/sink/rabbitmq"
)

type SinkConfig struct {
	RabbitIp					string
	RabbitPort					string
}

type Sink struct {
	connection 					*amqp.Connection
	channel 					*amqp.Channel
	funniestCitiesQueue 		*rabbitmq.RabbitInputQueue
	weekdayHistogramQueue 		*rabbitmq.RabbitInputQueue
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

	funniestCitiesQueue := rabbitmq.NewRabbitInputQueue(rabbitmq.FUNNIES_CITIES_QUEUE_NAME, ch)
	weekdayHistogramQueue := rabbitmq.NewRabbitInputQueue(rabbitmq.WEEKDAY_HISTOGRAM_QUEUE_NAME, ch)
	sink := &Sink {
		connection:				conn,
		channel:				ch,
		funniestCitiesQueue:	funniestCitiesQueue,
		weekdayHistogramQueue:  weekdayHistogramQueue,
	}

	return sink
}

func (sink *Sink) Run() {
	log.Infof("Starting to listen for results.")

	var wg sync.WaitGroup
	wg.Add(5)

	// TODO: Use this same logic for this and the other queues.
	go func() {
		for message := range sink.funniestCitiesQueue.ConsumeData() {
			log.Infof("Top 10 Funniest Cities: %s", string(message.Body))
			rabbitmq.AckMessage(&message, "FUNNIEST-CITIES")
			wg.Done()
		}
	}()

	go func() {
		for message := range sink.weekdayHistogramQueue.ConsumeData() {
			log.Infof("Reviews by Weekday: %s", string(message.Body))
			//rabbitmq.AckMessage(&message, "WEEKDAY-HISTOGRAM")
			wg.Done()
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
