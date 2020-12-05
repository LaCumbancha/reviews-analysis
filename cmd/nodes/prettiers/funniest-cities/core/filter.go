package core

import (
	"sync"
	"encoding/json"
	"github.com/streadway/amqp"
	
	log "github.com/sirupsen/logrus"
	props "github.com/LaCumbancha/reviews-analysis/cmd/common/properties"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
	rabbit "github.com/LaCumbancha/reviews-analysis/cmd/common/middleware"
)

type FilterConfig struct {
	RabbitIp		string
	RabbitPort		string
	FuncitFilters	int
	TopSize			int
}

type Filter struct {
	connection 		*amqp.Connection
	channel 		*amqp.Channel
	builder			*Builder
	inputQueue 		*rabbit.RabbitInputQueue
	outputQueue 	*rabbit.RabbitOutputQueue
	endSignals		int
}

func NewFilter(config FilterConfig) *Filter {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputQueue := rabbit.NewRabbitInputQueue(channel, props.FuncitFilterOutput)
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.FunniestCitiesPrettierOutput, comms.EndMessage(""), comms.EndSignals(1))

	filter := &Filter {
		connection:		connection,
		channel:		channel,
		builder:		NewBuilder(config.TopSize),
		inputQueue:		inputQueue,
		outputQueue:	outputQueue,
		endSignals:		config.FuncitFilters,
	}

	return filter
}

func (filter *Filter) Run() {
	log.Infof("Starting to listen for top funniest-cities data.")

	var distinctEndSignals = make(map[string]int)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		messageCounter := 0
		for message := range filter.inputQueue.ConsumeData() {
			messageBody := string(message.Body)

			if comms.IsEndMessage(messageBody) {
				newFinishReceived, allFinishReceived := comms.LastEndMessage(messageBody, distinctEndSignals, filter.endSignals)

				if newFinishReceived {
					log.Infof("End-Message #%d received.", len(distinctEndSignals))
				}

				if allFinishReceived {
					log.Infof("All End-Messages were received.")
					wg.Done()
				}

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
    filter.sendResults()

    // Sending End-Message to consumers.
    filter.outputQueue.PublishFinish()
}

func (filter *Filter) sendResults() {
	data, err := json.Marshal(filter.builder.BuildTopTen())
	if err != nil {
		log.Errorf("Error generating Json from best users results. Err: '%s'", err)
	} else {
		err := filter.outputQueue.PublishData(data)

		if err != nil {
			log.Errorf("Error sending best users results to output queue %s. Err: '%s'", filter.outputQueue.Name, err)
		} else {
			log.Infof("Best user results sent to output queue %s.", filter.outputQueue.Name)
		}
	}
}

func (filter *Filter) Stop() {
	log.Infof("Closing Funniest-Cities Prettier connections.")
	filter.connection.Close()
	filter.channel.Close()
}
