package core

import (
	"fmt"
	"sync"
	"github.com/streadway/amqp"
	"github.com/LaCumbancha/reviews-analysis/cmd/nodes/joiners/best-users/rabbitmq"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	props "github.com/LaCumbancha/reviews-analysis/cmd/common/properties"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
	rabbit "github.com/LaCumbancha/reviews-analysis/cmd/common/middleware"
)

const FLOW1 = "BEST_USERS"
const FLOW2 = "TOTAL_REVIEWS"

type JoinerConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	InputTopic			string
	StarsAggregators	int
	UserFilters 		int
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

	inputDirect1 := rabbit.NewRabbitInputDirect(ch, props.StarsAggregatorOutput, config.InputTopic, "")
	inputDirect2 := rabbit.NewRabbitInputDirect(ch, props.BestUsersFilterOutput, config.InputTopic, "")
	outputQueue := rabbitmq.NewRabbitOutputQueue(props.BestUsersJoinerOutput, config.Instance, ch)
	joiner := &Joiner {
		connection:		conn,
		channel:		ch,
		calculator:		NewCalculator(),
		inputDirect1:	inputDirect1,
		inputDirect2:	inputDirect2,
		outputQueue:	outputQueue,
		endSignals1:	config.StarsAggregators,
		endSignals2:	config.UserFilters,
	}

	return joiner
}

func (joiner *Joiner) Run() {
	var endSignals1Mutex = &sync.Mutex{}
	var endSignals2Mutex = &sync.Mutex{}

	var endSignals1 = make(map[string]int)
	var endSignals2 = make(map[string]int)

	var joinWg sync.WaitGroup
	var inputWg sync.WaitGroup

	// Receiving messages from the funny-business flow.
	inputWg.Add(1)
	go func() {
		log.Infof("Starting to listen for users 5-stars reviews data.")
		inputDirectChannel, err := joiner.inputDirect1.ConsumeData()
		if err != nil {
			log.Fatalf("Error receiving data from direct-exchange %s. Err: '%s'", joiner.inputDirect1.Exchange, err)
		}

		bulkCounter := 0
		for message := range inputDirectChannel {
			messageBody := string(message.Body)

			if comms.IsEndMessage(messageBody) {
				joiner.processEndSignal(FLOW1, messageBody, joiner.endSignals1, endSignals1, endSignals1Mutex, &inputWg)
			} else {
				bulkCounter++
				logb.Instance().Infof(fmt.Sprintf("Best users data bulk #%d received.", bulkCounter), bulkCounter)

				inputWg.Add(1)
				go func(bulkNumber int, bulk string) {
					joiner.calculator.AddBestUser(bulkNumber, bulk)
					inputWg.Done()
				}(bulkCounter, messageBody)
			}
		}
	}()

	// Receiving messages from the city-business flow.
	inputWg.Add(1)
	go func() {
		log.Infof("Starting to listen for users reviews data.")
		inputDirectChannel, err := joiner.inputDirect2.ConsumeData()
		if err != nil {
			log.Fatalf("Error receiving data from direct-exchange %s. Err: '%s'", joiner.inputDirect2.Exchange, err)
		}

		bulkCounter := 0
		for message := range inputDirectChannel {
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
    	go func(messageNumber int, bestUser comms.UserData) {
    		joiner.outputQueue.PublishData(messageNumber, bestUser)
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
	log.Infof("Closing Best-Users Joiner connections.")
	joiner.connection.Close()
	joiner.channel.Close()
}
