package common

import (
	"fmt"
	"sync"
	"encoding/json"
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/aggregators/funny-business/rabbitmq"
)

type AggregatorConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	InputTopic			string
	FunbizFilters 		int
	FuncitJoiners		int
}

type Aggregator struct {
	connection 		*amqp.Connection
	channel 		*amqp.Channel
	calculator		*Calculator
	inputDirect 	*rabbitmq.RabbitInputDirect
	outputDirect 	*rabbitmq.RabbitOutputDirect
	endSignals		int
}

func NewAggregator(config AggregatorConfig) *Aggregator {
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

	inputDirect := rabbitmq.NewRabbitInputDirect(rabbitmq.INPUT_EXCHANGE_NAME, config.InputTopic, ch)
	outputDirect := rabbitmq.NewRabbitOutputDirect(rabbitmq.OUTPUT_EXCHANGE_NAME, config.Instance, config.FuncitJoiners, ch)
	aggregator := &Aggregator {
		connection:		conn,
		channel:		ch,
		calculator:		NewCalculator(),
		inputDirect:	inputDirect,
		outputDirect:	outputDirect,
		endSignals:		config.FunbizFilters,
	}

	return aggregator
}

func (aggregator *Aggregator) Run() {
	log.Infof("Starting to listen for funny-business data.")

	var endSignalsMutex = &sync.Mutex{}
	var endSignals = make(map[string]int)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for message := range aggregator.inputDirect.ConsumeData() {
			messageBody := string(message.Body)

			if rabbitmq.IsEndMessage(messageBody) {
				aggregator.processEndSignal(messageBody, endSignals, endSignalsMutex, &wg)
			} else {
				log.Infof("Data '%s' received.", messageBody)

				wg.Add(1)
				go func() {
					aggregator.calculator.Aggregate(messageBody)
					wg.Done()
				}()
			}
		}
	}()
	
    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are received.
    wg.Wait()

    for _, aggregatedData := range aggregator.calculator.RetrieveData() {
		wg.Add(1)
		go aggregator.sendAggregatedData(aggregatedData, &wg)
	}

    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are sent.
    wg.Wait()

    // Sending End-Message to consumers.
    aggregator.outputDirect.PublishFinish()
}

func (aggregator *Aggregator) processEndSignal(newMessage string, endSignals map[string]int, mutex *sync.Mutex, wg *sync.WaitGroup) {
	mutex.Lock()
	endSignals[newMessage] = endSignals[newMessage] + 1
	newSignal := endSignals[newMessage] == 1
	signalsReceived := len(endSignals)
	mutex.Unlock()

	log.Infof("End-Message #%d received.", signalsReceived)

	// Waiting for the total needed End-Signals to send the own End-Message.
	if (signalsReceived == aggregator.endSignals) && newSignal {
		log.Infof("All End-Messages were received.")
		wg.Done()
	}
}

func (aggregator *Aggregator) sendAggregatedData(aggregatedData rabbitmq.FunnyBusinessData, wg *sync.WaitGroup) {
	data, err := json.Marshal(aggregatedData)
	if err != nil {
		log.Errorf("Error generating Json from (%s). Err: '%s'", aggregatedData, err)
	} else {
		aggregator.outputDirect.PublishData(data, aggregatedData.BusinessId)
	}
	wg.Done()
}

func (aggregator *Aggregator) Stop() {
	log.Infof("Closing Funny-Business Aggregator connections.")
	aggregator.connection.Close()
	aggregator.channel.Close()
}
