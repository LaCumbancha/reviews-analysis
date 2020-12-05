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

type PrettierConfig struct {
	RabbitIp		string
	RabbitPort		string
	FuncitFilters	int
	TopSize			int
}

type Prettier struct {
	connection 		*amqp.Connection
	channel 		*amqp.Channel
	builder			*Builder
	inputQueue 		*rabbit.RabbitInputQueue
	outputQueue 	*rabbit.RabbitOutputQueue
	endSignals		int
}

func NewPrettier(config PrettierConfig) *Prettier {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputQueue := rabbit.NewRabbitInputQueue(channel, props.FuncitTopOutput)
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.FunniestCitiesPrettierOutput, comms.EndMessage(""), comms.EndSignals(1))

	prettier := &Prettier {
		connection:		connection,
		channel:		channel,
		builder:		NewBuilder(config.TopSize),
		inputQueue:		inputQueue,
		outputQueue:	outputQueue,
		endSignals:		config.FuncitFilters,
	}

	return prettier
}

func (prettier *Prettier) Run() {
	log.Infof("Starting to listen for top funniest-cities data.")

	var distinctEndSignals = make(map[string]int)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		messageCounter := 0
		for message := range prettier.inputQueue.ConsumeData() {
			messageBody := string(message.Body)

			if comms.IsEndMessage(messageBody) {
				newFinishReceived, allFinishReceived := comms.LastEndMessage(messageBody, distinctEndSignals, prettier.endSignals)

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
					prettier.builder.Save(message)
					wg.Done()
				}(messageBody)
			}
		}
	}()
	
    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are received.
    wg.Wait()

    // Sending results
    prettier.sendResults()

    // Sending End-Message to consumers.
    prettier.outputQueue.PublishFinish()
}

func (prettier *Prettier) sendResults() {
	data, err := json.Marshal(prettier.builder.BuildTopTen())
	if err != nil {
		log.Errorf("Error generating Json from best users results. Err: '%s'", err)
	} else {
		err := prettier.outputQueue.PublishData(data)

		if err != nil {
			log.Errorf("Error sending best users results to output queue %s. Err: '%s'", prettier.outputQueue.Name, err)
		} else {
			log.Infof("Best user results sent to output queue %s.", prettier.outputQueue.Name)
		}
	}
}

func (prettier *Prettier) Stop() {
	log.Infof("Closing Funniest-Cities Prettier connections.")
	prettier.connection.Close()
	prettier.channel.Close()
}
