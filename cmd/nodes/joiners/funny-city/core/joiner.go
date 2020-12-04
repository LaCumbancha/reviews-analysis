package core

import (
	"fmt"
	"sync"
	"github.com/streadway/amqp"
	"github.com/LaCumbancha/reviews-analysis/cmd/nodes/joiners/funny-city/rabbitmq"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	props "github.com/LaCumbancha/reviews-analysis/cmd/common/properties"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
	rabbit "github.com/LaCumbancha/reviews-analysis/cmd/common/middleware"
)

const FLOW1 = "FUNBIZ"
const FLOW2 = "CITBIZ"

type JoinerConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	InputTopic			string
	FunbizAggregators 	int
	CitbizMappers		int
	FuncitAggregators	int
	OutputBulkSize		int
}

type Joiner struct {
	connection 		*amqp.Connection
	channel 		*amqp.Channel
	calculator		*Calculator
	inputDirect1 	*rabbit.RabbitInputDirect
	inputDirect2 	*rabbit.RabbitInputDirect
	outputDirect 	*rabbitmq.RabbitOutputDirect
	endSignals1		int
	endSignals2		int
}

func NewJoiner(config JoinerConfig) *Joiner {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputDirect1 := rabbit.NewRabbitInputDirect(channel, props.FunbizAggregatorOutput, config.InputTopic, "")
	inputDirect2 := rabbit.NewRabbitInputDirect(channel, props.CitbizMapperOutput, config.InputTopic, "")
	outputDirect := rabbitmq.NewRabbitOutputDirect(props.FuncitJoinerOutput, config.Instance, config.FuncitAggregators, channel)

	joiner := &Joiner {
		connection:		connection,
		channel:		channel,
		calculator:		NewCalculator(config.OutputBulkSize),
		inputDirect1:	inputDirect1,
		inputDirect2:	inputDirect2,
		outputDirect:	outputDirect,
		endSignals1:	config.FunbizAggregators,
		endSignals2:	config.CitbizMappers,
	}

	return joiner
}

func (joiner *Joiner) Run() {
	var endSignals1Mutex = &sync.Mutex{}
	var endSignals2Mutex = &sync.Mutex{}

	var endSignals1 = make(map[string]int)
	var endSignals2 = make(map[string]int)

	var inputWg sync.WaitGroup
	var joinWg sync.WaitGroup

	// Receiving messages from the funny-business flow.
	inputWg.Add(1)
	go func() {
		log.Infof("Starting to listen for funny-business data.")

		bulkCounter := 0
		for message := range joiner.inputDirect1.ConsumeData() {
			messageBody := string(message.Body)

			if comms.IsEndMessage(messageBody) {
				joiner.processEndSignal(FLOW1, messageBody, joiner.endSignals1, endSignals1, endSignals1Mutex, &inputWg)
			} else {
				bulkCounter++
				logb.Instance().Infof(fmt.Sprintf("Funbiz data bulk #%d received.", bulkCounter), bulkCounter)

				inputWg.Add(1)
				go func(bulkNumber int, bulk string) {
					joiner.calculator.AddFunnyBusiness(bulkNumber, bulk)
					inputWg.Done()
				}(bulkCounter, messageBody)
			}
		}
	}()

	// Receiving messages from the city-business flow.
	inputWg.Add(1)
	go func() {
		log.Infof("Starting to listen for city-business data.")

		bulkCounter := 0
		for message := range joiner.inputDirect2.ConsumeData() {
			messageBody := string(message.Body)

			if comms.IsEndMessage(messageBody) {
				joiner.processEndSignal(FLOW2, messageBody, joiner.endSignals2, endSignals2, endSignals2Mutex, &inputWg)
			} else {
				bulkCounter++
				logb.Instance().Infof(fmt.Sprintf("Citbiz data bulk #%d received.", bulkCounter), bulkCounter * 5)

				inputWg.Add(1)
				go func(bulkNumber int, bulk string) {
					joiner.calculator.AddCityBusiness(bulkNumber, bulk)
					inputWg.Done()
				}(bulkCounter, messageBody)
			}
		}
	}()

	// Using WaitGroups to avoid closing the RabbitMQ connection before all messages are received.
    inputWg.Wait()

    // Processing last join matches.
    joiner.fetchJoinMatches(&joinWg)

    // Using WaitGroups to avoid closing the RabbitMQ connection before all joins are processed and sent.
    joinWg.Wait()

    // Sending End-Message to consumers.
    joiner.outputDirect.PublishFinish()
}

func (joiner *Joiner) fetchJoinMatches(joinWg *sync.WaitGroup) {
	outputBulks := 0
	joinMatches := joiner.calculator.RetrieveMatches()

	if len(joinMatches) == 0 {
    	log.Warnf("No join matches to send.")
    }

    for _, joinedData := range joinMatches {
    	outputBulks++

    	joinWg.Add(1)
    	go func(bulkNumber int, bulk []comms.FunnyCityData) {
    		joiner.outputDirect.PublishData(bulkNumber, bulk)
    		joinWg.Done()
    	}(outputBulks, joinedData)
	}
}

func (joiner *Joiner) processEndSignal(flow string, newMessage string, expectedEndSignals int, receivedEndSignals map[string]int, mutex *sync.Mutex, wg *sync.WaitGroup) {
	mutex.Lock()
	receivedEndSignals[newMessage] = receivedEndSignals[newMessage] + 1
	newSignal := receivedEndSignals[newMessage] == 1
	signalsReceived := len(receivedEndSignals)
	mutex.Unlock()

	if newSignal {
		log.Infof("End-Message #%d from the %s flow received.", signalsReceived, flow)
	}

	// Waiting for the total needed End-Signals to send the own End-Message.
	if (signalsReceived == expectedEndSignals) && newSignal {
		log.Infof("All End-Messages from the %s flow were received.", flow)
		wg.Done()
	}
}

func (joiner *Joiner) Stop() {
	log.Infof("Closing Funny-City Joiner connections.")
	joiner.connection.Close()
	joiner.channel.Close()
}
