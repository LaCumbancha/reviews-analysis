package core

import (
	"fmt"
	"sync"
	"github.com/streadway/amqp"
	"github.com/LaCumbancha/reviews-analysis/cmd/nodes/joiners/bot-users/rabbitmq"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	props "github.com/LaCumbancha/reviews-analysis/cmd/common/properties"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
	rabbit "github.com/LaCumbancha/reviews-analysis/cmd/common/middleware"
)

const FLOW1 = "BOT_USERS"
const FLOW2 = "TOTAL_REVIEWS"

type JoinerConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	InputTopic			string
	DishashFilters		int
	BotUsersFilters 	int
}

type Joiner struct {
	connection 		*amqp.Connection
	channel 		*amqp.Channel
	calculator		*Calculator
	inputDirect1 	*rabbit.RabbitInputDirect
	inputDirect2 	*rabbit.RabbitInputDirect
	outputQueue 	*rabbitmq.RabbitOutputQueue
	endSignals1		int
	endSignals2		int
}

func NewJoiner(config JoinerConfig) *Joiner {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputDirect1 := rabbit.NewRabbitInputDirect(channel, props.DishashFilterOutput, config.InputTopic, "")
	inputDirect2 := rabbit.NewRabbitInputDirect(channel, props.BotUsersFilterOutput, config.InputTopic, "")
	outputQueue := rabbitmq.NewRabbitOutputQueue(props.BotUsersJoinerOutput, config.Instance, channel)

	joiner := &Joiner {
		connection:		connection,
		channel:		channel,
		calculator:		NewCalculator(),
		inputDirect1:	inputDirect1,
		inputDirect2:	inputDirect2,
		outputQueue:	outputQueue,
		endSignals1:	config.DishashFilters,
		endSignals2:	config.BotUsersFilters,
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
		log.Infof("Starting to listen for bot users with only one text.")

		bulkCounter := 0
		for message := range joiner.inputDirect1.ConsumeData() {
			messageBody := string(message.Body)

			if comms.IsEndMessage(messageBody) {
				joiner.processEndSignal(FLOW1, messageBody, joiner.endSignals1, endSignals1, endSignals1Mutex, &inputWg)
			} else {
				bulkCounter++
				logb.Instance().Infof(fmt.Sprintf("Bot users data bulk #%d received.", bulkCounter), bulkCounter)

				inputWg.Add(1)
				go func(bulkNumber int, bulk string) {
					joiner.calculator.AddBotUser(bulkNumber, bulk)
					inputWg.Done()
				}(bulkCounter, messageBody)
			}
		}
	}()

	// Receiving messages from the city-business flow.
	inputWg.Add(1)
	go func() {
		log.Infof("Starting to listen for users reviews data.")

		bulkCounter := 0
		for message := range joiner.inputDirect2.ConsumeData() {
			messageBody := string(message.Body)

			if comms.IsEndMessage(messageBody) {
				joiner.processEndSignal(FLOW2, messageBody, joiner.endSignals2, endSignals2, endSignals2Mutex, &inputWg)
			} else {
				bulkCounter++
				logb.Instance().Infof(fmt.Sprintf("Common users data bulk #%d received.", bulkCounter), bulkCounter)

				inputWg.Add(1)
				go func(bulkNumber int, bulk string) {
					joiner.calculator.AddUser(bulkNumber, bulk)
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
    joiner.outputQueue.PublishFinish()
}

func (joiner *Joiner) fetchJoinMatches(joinWg *sync.WaitGroup) {
	joinMatches := joiner.calculator.RetrieveMatches()

	if len(joinMatches) == 0 {
    	log.Warnf("No join match to send.")
    }

    messageCounter := 0
    for _, joinedData := range joinMatches {
    	messageCounter++

    	joinWg.Add(1)
    	go func(messageNumber int, botUser comms.UserData) {
    		joiner.outputQueue.PublishData(messageNumber, botUser)
    		joinWg.Done()
    	}(messageCounter, joinedData)
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
	log.Infof("Closing Bot-Users Joiner connections.")
	joiner.connection.Close()
	joiner.channel.Close()
}
