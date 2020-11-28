package common

import (
	"fmt"
	"sync"
	"encoding/json"
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/joiners/funny-city/rabbitmq"
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
		calculator:		NewCalculator(),
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
		for message := range joiner.inputDirect1.ConsumeData() {
			messageBody := string(message.Body)

			if rabbitmq.IsEndMessage(messageBody) {
				joiner.processEndSignal(FLOW1, messageBody, joiner.endSignals1, endSignals1, endSignals1Mutex, &inputWg)
			} else {
				log.Infof("Data '%s' received.", messageBody)

				inputWg.Add(1)
				go func() {
					joiner.calculator.AddFunnyBusiness(messageBody)
					joiner.fetchJoinMatches(&joinWg)
					inputWg.Done()
				}()
			}
		}
	}()

	// Receiving messages from the city-business flow.
	inputWg.Add(1)
	go func() {
		log.Infof("Starting to listen for city-business data.")
		for message := range joiner.inputDirect2.ConsumeData() {
			messageBody := string(message.Body)

			if rabbitmq.IsEndMessage(messageBody) {
				joiner.processEndSignal(FLOW2, messageBody, joiner.endSignals2, endSignals2, endSignals2Mutex, &inputWg)
			} else {
				log.Infof("Data '%s' received.", messageBody)

				inputWg.Add(1)
				go func() {
					joiner.calculator.AddCityBusiness(messageBody)
					joiner.fetchJoinMatches(&joinWg)
					inputWg.Done()
				}()
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
	joinMatches := joiner.calculator.RetrieveMatches()

	if len(joinMatches) == 0 {
    	log.Tracef("No new join match to send in this round.")
    }

    for _, joinedData := range joinMatches {
    	joinWg.Add(1)
    	log.Infof("Starting to send joined funny city data (business %s).", joinedData.City)
		go joiner.sendJoinedData(joinedData, joinWg)
	}
}

func (joiner *Joiner) processEndSignal(flow string, newMessage string, expectedEndSignals int, receivedEndSignals map[string]int, mutex *sync.Mutex, wg *sync.WaitGroup) {
	mutex.Lock()
	receivedEndSignals[newMessage] = receivedEndSignals[newMessage] + 1
	newSignal := receivedEndSignals[newMessage] == 1
	signalsReceived := len(receivedEndSignals)
	mutex.Unlock()

	log.Infof("End-Message #%d from the %s flow received.", signalsReceived, flow)

	// Waiting for the total needed End-Signals to send the own End-Message.
	if (signalsReceived == expectedEndSignals) && newSignal {
		log.Infof("All End-Messages from the %s flow were received.", flow)
		wg.Done()
	}
}

func (joiner *Joiner) sendJoinedData(joinedData rabbitmq.FunnyCityData, wg *sync.WaitGroup) {
	data, err := json.Marshal(joinedData)
	if err != nil {
		log.Errorf("Error generating Json from (%s). Err: '%s'", joinedData, err)
	} else {
		joiner.outputDirect.PublishData(data, joinedData.City)
	}
	wg.Done()
}

func (joiner *Joiner) Stop() {
	log.Infof("Closing Funny-City Joiner connections.")
	joiner.connection.Close()
	joiner.channel.Close()
}
