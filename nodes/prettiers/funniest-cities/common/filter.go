package common

import (
	"fmt"
	"sync"
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/prettiers/funniest-cities/rabbitmq"
)

type FilterConfig struct {
	RabbitIp		string
	RabbitPort		string
	FuncitFilters	int
}

type Filter struct {
	connection 		*amqp.Connection
	channel 		*amqp.Channel
	builder		*Builder
	inputQueue 		*rabbitmq.RabbitInputQueue
	outputQueue 	*rabbitmq.RabbitOutputQueue
	endSignals		int
}

func NewFilter(config FilterConfig) *Filter {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%s/", config.RabbitIp, config.RabbitPort))
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ at (%s, %s). Err: '%s'", config.RabbitIp, config.RabbitPort, err)
	} else {
		log.Infof("Connected to RabbitMQ at (%s, %s).", config.RabbitIp, config.RabbitPort)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a RabbitMQ channel. Err: '%s'", err)
	} else {
		log.Infof("RabbitMQ channel opened.")
	}

	inputQueue := rabbitmq.NewRabbitInputQueue(rabbitmq.INPUT_QUEUE_NAME, ch)
	outputQueue := rabbitmq.NewRabbitOutputQueue(rabbitmq.OUTPUT_QUEUE_NAME, ch)
	filter := &Filter {
		connection:		conn,
		channel:		ch,
		builder:		NewBuilder(),
		inputQueue:		inputQueue,
		outputQueue:	outputQueue,
		endSignals:		config.FuncitFilters,
	}

	return filter
}

func (filter *Filter) Run() {
	log.Infof("Starting to listen for top funniest-cities data.")

	var endSignalsMutex = &sync.Mutex{}
	var endSignals = make(map[string]int)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		messageCounter := 0
		for message := range filter.inputQueue.ConsumeData() {
			messageBody := string(message.Body)

			if rabbitmq.IsEndMessage(messageBody) {
				filter.processEndSignal(messageBody, endSignals, endSignalsMutex, &wg)
			} else {
				messageCounter++
				log.Infof("Funniest city #%d received.", messageCounter)

				wg.Add(1)
				go func(message string) {
					filter.builder.Save(message)
					wg.Done()
				}(messageBody)
			}
		}
	}()
	
    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are received.
    wg.Wait()

    // Sending results
    filter.outputQueue.PublishData(filter.builder.BuildTopTen())

    // Sending End-Message to consumers.
    filter.outputQueue.PublishFinish()
}

func (filter *Filter) processEndSignal(newMessage string, endSignals map[string]int, mutex *sync.Mutex, wg *sync.WaitGroup) {
	mutex.Lock()
	endSignals[newMessage] = endSignals[newMessage] + 1
	newSignal := endSignals[newMessage] == 1
	signalsReceived := len(endSignals)
	mutex.Unlock()

	if newSignal {
		log.Infof("End-Message #%d received.", signalsReceived)
	}

	// Waiting for the total needed End-Signals to send the own End-Message.
	if (signalsReceived == filter.endSignals) && newSignal {
		log.Infof("All End-Messages were received.")
		wg.Done()
	}
}

func (filter *Filter) Stop() {
	log.Infof("Closing Funniest-Cities Prettier connections.")
	filter.connection.Close()
	filter.channel.Close()
}
