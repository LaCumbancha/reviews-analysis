package core

import (
	"fmt"
	"sync"
	"encoding/json"
	"github.com/streadway/amqp"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	props "github.com/LaCumbancha/reviews-analysis/cmd/common/properties"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
	rabbit "github.com/LaCumbancha/reviews-analysis/cmd/common/middleware"
)

type FilterConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	FuncitAggregators	int
	TopSize				int
}

type Filter struct {
	connection 		*amqp.Connection
	channel 		*amqp.Channel
	calculator		*Calculator
	inputQueue 		*rabbit.RabbitInputQueue
	outputQueue 	*rabbit.RabbitOutputQueue
	endSignals		int
}

func NewFilter(config FilterConfig) *Filter {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputQueue := rabbit.NewRabbitInputQueue(channel, props.FuncitAggregatorOutput)
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.FuncitFilterOutput, comms.EndMessage(config.Instance), comms.EndSignals(1))

	filter := &Filter {
		connection:		connection,
		channel:		channel,
		calculator:		NewCalculator(config.TopSize),
		inputQueue:		inputQueue,
		outputQueue:	outputQueue,
		endSignals:		config.FuncitAggregators,
	}

	return filter
}

func (filter *Filter) Run() {
	log.Infof("Starting to listen for funny-city data.")

	var endSignalsMutex = &sync.Mutex{}
	var endSignals = make(map[string]int)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		bulkCounter := 0
		for message := range filter.inputQueue.ConsumeData() {
			messageBody := string(message.Body)

			if comms.IsEndMessage(messageBody) {
				filter.processEndSignal(messageBody, endSignals, endSignalsMutex, &wg)
			} else {
				bulkCounter++
				logb.Instance().Infof(fmt.Sprintf("Funcit data bulk #%d received.", bulkCounter), bulkCounter)

				wg.Add(1)
				go func(bulkNumber int, bulk string) {
					filter.calculator.Save(bulkNumber, bulk)
					wg.Done()
				}(bulkCounter, messageBody)
			}
		}
	}()
	
    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are received.
    wg.Wait()

    cityCounter := 0
    for _, cityData := range filter.calculator.RetrieveTopTen() {
    	cityCounter++

    	wg.Add(1)
    	go filter.sendTopTenData(cityCounter, cityData, &wg)
	}

    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are sent.
    wg.Wait()

    // Sending End-Message to consumers.
    filter.outputQueue.PublishFinish()
}

func (filter *Filter) sendTopTenData(cityNumber int, topTenCity comms.FunnyCityData, wg *sync.WaitGroup) {
	data, err := json.Marshal(topTenCity)
	if err != nil {
		log.Errorf("Error generating Json from funniest city #%d data. Err: '%s'", cityNumber, err)
	} else {
		err := filter.outputQueue.PublishData(data)

		if err != nil {
			log.Errorf("Error sending funniest city #%d data to output queue %s. Err: '%s'", cityNumber, filter.outputQueue.Name, err)
		} else {
			log.Infof("Funniest city #%d data sent to output queue %s.", cityNumber, filter.outputQueue.Name)
		}
	}

	wg.Done()
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
	log.Infof("Closing Funny-City Filter connections.")
	filter.connection.Close()
	filter.channel.Close()
}
