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

type MapperConfig struct {
	RabbitIp			string
	RabbitPort			string
	WeekdayAggregators 	int
}

type Mapper struct {
	connection 		*amqp.Connection
	channel 		*amqp.Channel
	builder			*Builder
	inputQueue 		*rabbit.RabbitInputQueue
	outputQueue 	*rabbit.RabbitOutputQueue
	endSignals 		int
}

func NewMapper(config MapperConfig) *Mapper {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputQueue := rabbit.NewRabbitInputQueue(channel, props.WeekdayAggregatorOutput)
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.WeekdayHistogramPrettierOutput, comms.EndMessage(""), comms.EndSignals(1))

	mapper := &Mapper {
		connection:		connection,
		channel:		channel,
		builder:		NewBuilder(),
		inputQueue:		inputQueue,
		outputQueue:	outputQueue,
		endSignals:		config.WeekdayAggregators,
	}

	return mapper
}

func (mapper *Mapper) Run() {
	log.Infof("Starting to listen for weekday reviews data.")

	var endSignalsMutex = &sync.Mutex{}
	var endSignals = make(map[string]int)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		messageCounter := 0
		for message := range mapper.inputQueue.ConsumeData() {
			messageBody := string(message.Body)

			if comms.IsEndMessage(messageBody) {
				mapper.processEndSignal(messageBody, endSignals, endSignalsMutex, &wg)
			} else {
				messageCounter++
				log.Infof("Aggregated weekday reviews #%d received.", messageCounter)

				wg.Add(1)
				go func(message string) {
					mapper.builder.Save(message)
					wg.Done()
				}(messageBody)
			}
		}
	}()
	
    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are sent.
    wg.Wait()

    // Sending results
    mapper.sendResults()

    // Publishing end messages.
    mapper.outputQueue.PublishFinish()
}

func (mapper *Mapper) processEndSignal(newMessage string, endSignals map[string]int, mutex *sync.Mutex, wg *sync.WaitGroup) {
	mutex.Lock()
	endSignals[newMessage] = endSignals[newMessage] + 1
	newSignal := endSignals[newMessage] == 1
	signalsReceived := len(endSignals)
	mutex.Unlock()

	if newSignal {
		log.Infof("End-Message #%d received.", signalsReceived)
	}

	// Waiting for the total needed End-Signals to send the own End-Message.
	if (signalsReceived == mapper.endSignals) && newSignal {
		log.Infof("All End-Messages were received.")
		wg.Done()
	}
}

func (mapper *Mapper) sendResults() {
	data, err := json.Marshal(mapper.builder.BuildData())
	if err != nil {
		log.Errorf("Error generating Json from best users results. Err: '%s'", err)
	} else {
		err := mapper.outputQueue.PublishData(data)

		if err != nil {
			log.Errorf("Error sending best users results to output queue %s. Err: '%s'", mapper.outputQueue.Name, err)
		} else {
			log.Infof("Best user results sent to output queue %s.", mapper.outputQueue.Name)
		}
	}
}

func (mapper *Mapper) Stop() {
	log.Infof("Closing Weekday-Histogram Prettier connections.")
	mapper.connection.Close()
	mapper.channel.Close()
}
