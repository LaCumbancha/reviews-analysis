package core

import (
	"sync"
	"github.com/streadway/amqp"
	"github.com/LaCumbancha/reviews-analysis/cmd/nodes/outputs/sink/rabbitmq"

	log "github.com/sirupsen/logrus"
	props "github.com/LaCumbancha/reviews-analysis/cmd/common/properties"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
	rabbit "github.com/LaCumbancha/reviews-analysis/cmd/common/middleware"
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
	topUsersQueue 				*rabbitmq.RabbitInputQueue
	bestUsersQueue 				*rabbitmq.RabbitInputQueue
	botUsersQueue 				*rabbitmq.RabbitInputQueue
}

func NewSink(config SinkConfig) *Sink {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	funniestCitiesQueue := rabbitmq.NewRabbitInputQueue(props.FunniestCitiesPrettierOutput, channel)
	weekdayHistogramQueue := rabbitmq.NewRabbitInputQueue(props.WeekdayHistogramPrettierOutput, channel)
	topUsersQueue := rabbitmq.NewRabbitInputQueue(props.TopUsersPrettierOutput, channel)
	bestUsersQueue := rabbitmq.NewRabbitInputQueue(props.BestUsersPrettierOutput, channel)
	botUsersQueue := rabbitmq.NewRabbitInputQueue(props.BotUsersPrettierOutput, channel)

	sink := &Sink {
		connection:				connection,
		channel:				channel,
		funniestCitiesQueue:	funniestCitiesQueue,
		weekdayHistogramQueue:  weekdayHistogramQueue,
		topUsersQueue:			topUsersQueue,
		bestUsersQueue:			bestUsersQueue,
		botUsersQueue:			botUsersQueue,
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
			messageBody := string(message.Body)

			if comms.IsEndMessage(messageBody) {
				log.Infof("End-Message received from the Funniest Cities flow.")
				wg.Done()
			} else {
				log.Infof(messageBody)
			}
		}
	}()

	go func() {
		for message := range sink.weekdayHistogramQueue.ConsumeData() {
			messageBody := string(message.Body)

			if comms.IsEndMessage(messageBody) {
				log.Infof("End-Message received from the Weekday Histogram flow.")
				wg.Done()
			} else {
				log.Infof(messageBody)
			}
		}
	}()

	go func() {
		for message := range sink.topUsersQueue.ConsumeData() {
			messageBody := string(message.Body)

			if comms.IsEndMessage(messageBody) {
				log.Infof("End-Message received from the Top-Users flow.")
				wg.Done()
			} else {
				log.Infof(messageBody)
			}
		}
	}()

	go func() {
		for message := range sink.bestUsersQueue.ConsumeData() {
			messageBody := string(message.Body)

			if comms.IsEndMessage(messageBody) {
				log.Infof("End-Message received from the Best-Users flow.")
				wg.Done()
			} else {
				log.Infof(messageBody)
			}
		}
	}()

	go func() {
		for message := range sink.botUsersQueue.ConsumeData() {
			messageBody := string(message.Body)
			
			if comms.IsEndMessage(messageBody) {
				log.Infof("End-Message received from the Bot-Users flow.")
				wg.Done()
			} else {
				log.Infof(messageBody)
			}
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
