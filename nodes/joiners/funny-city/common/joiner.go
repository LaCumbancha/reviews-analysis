package common

import (
	"fmt"
	"sync"
	"github.com/streadway/amqp"
	"github.com/LaCumbancha/reviews-analysis/nodes/joiners/funny-city/rabbitmq"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/nodes/joiners/funny-city/logger"
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
	inputDirect1 	*rabbitmq.RabbitInputDirect
	inputDirect2 	*rabbitmq.RabbitInputDirect
	outputDirect 	*rabbitmq.RabbitOutputDirect
	endSignals1		int
	endSignals2		int
}

func NewJoiner(config JoinerConfig) *Joiner {
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

	inputDirect1 := rabbitmq.NewRabbitInputDirect(rabbitmq.INPUT_EXCHANGE1_NAME, config.InputTopic, ch)
	inputDirect2 := rabbitmq.NewRabbitInputDirect(rabbitmq.INPUT_EXCHANGE2_NAME, config.InputTopic, ch)
	outputDirect := rabbitmq.NewRabbitOutputDirect(rabbitmq.OUTPUT_EXCHANGE_NAME, config.Instance, config.FuncitAggregators, ch)
	joiner := &Joiner {
		connection:		conn,
		channel:		ch,
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
		bulkCounter1 := 0
		log.Infof("Starting to listen for funny-business data.")
		for message := range joiner.inputDirect1.ConsumeData() {
			messageBody := string(message.Body)

			if rabbitmq.IsEndMessage(messageBody) {
				joiner.processEndSignal(FLOW1, messageBody, joiner.endSignals1, endSignals1, endSignals1Mutex, &inputWg)
			} else {
				bulkCounter1++
				logb.Instance().Infof(fmt.Sprintf("Funbiz data bulk #%d received.", bulkCounter1), bulkCounter1)

				inputWg.Add(1)
				go func(bulkNumber int) {
					joiner.calculator.AddFunnyBusiness(bulkNumber, messageBody)
					inputWg.Done()
				}(bulkCounter1)
			}
		}
	}()

	// Receiving messages from the city-business flow.
	inputWg.Add(1)
	go func() {
		bulkCounter2 := 0
		log.Infof("Starting to listen for city-business data.")
		for message := range joiner.inputDirect2.ConsumeData() {
			messageBody := string(message.Body)

			if rabbitmq.IsEndMessage(messageBody) {
				joiner.processEndSignal(FLOW2, messageBody, joiner.endSignals2, endSignals2, endSignals2Mutex, &inputWg)
			} else {
				bulkCounter2++
				logb.Instance().Infof(fmt.Sprintf("Citbiz data bulk #%d received.", bulkCounter2), bulkCounter2)

				inputWg.Add(1)
				go func(bulkNumber int) {
					joiner.calculator.AddCityBusiness(bulkNumber, messageBody)
					inputWg.Done()
				}(bulkCounter2)
			}
		}
	}()

	// Using WaitGroups to avoid closing the RabbitMQ connection before all messages are received.
    inputWg.Wait()

    // Processing last join matches.
    joiner.sendJoinMatches(&joinWg)

    // Using WaitGroups to avoid closing the RabbitMQ connection before all joins are processed and sent.
    joinWg.Wait()

    // Sending End-Message to consumers.
    joiner.outputDirect.PublishFinish()
}

func (joiner *Joiner) sendJoinMatches(joinWg *sync.WaitGroup) {
	outputBulks := 0
	joinMatches := joiner.calculator.RetrieveMatches()

	if len(joinMatches) == 0 {
    	log.Warnf("No join matches to send.")
    }

    for _, joinedData := range joinMatches {
    	outputBulks++
    	joinWg.Add(1)
    	go joiner.outputDirect.PublishData(outputBulks, joinedData)
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
