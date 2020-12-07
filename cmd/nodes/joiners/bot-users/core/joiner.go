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

type JoinerConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	WorkersPool 		int
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
	outputQueue 	*rabbit.RabbitOutputQueue
	endSignals1		int
	endSignals2		int
}

func NewJoiner(config JoinerConfig) *Joiner {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputDirect1 := rabbit.NewRabbitInputDirect(channel, props.DishashFilterOutput, config.InputTopic, "")
	inputDirect2 := rabbit.NewRabbitInputDirect(channel, props.BotUsersFilterOutput, config.InputTopic, "")
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.BotUsersJoinerOutput, comms.EndMessage(config.Instance), comms.EndSignals(1))

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
	var distinctEndSignals1 = make(map[string]int)
	var distinctEndSignals2 = make(map[string]int)

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
				newFinishReceived, allFinishReceived := comms.LastEndMessage(messageBody, distinctEndSignals1, joiner.endSignals1)

				if newFinishReceived {
					log.Infof("End-Message #%d received.", len(distinctEndSignals1))
				}

				if allFinishReceived {
					log.Infof("All End-Messages were received.")
					inputWg.Done()
				}

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
				newFinishReceived, allFinishReceived := comms.LastEndMessage(messageBody, distinctEndSignals2, joiner.endSignals2)

				if newFinishReceived {
					log.Infof("End-Message #%d received.", len(distinctEndSignals2))
				}

				if allFinishReceived {
					log.Infof("All End-Messages were received.")
					inputWg.Done()
				}

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
    	go joiner.sendJoinedData(messageCounter, joinedData, joinWg)
	}
}

func (joiner *Joiner) sendJoinedData(messageNumber int, joinedData comms.UserData, wg *sync.WaitGroup) {
	data, err := json.Marshal(joinedData)
	if err != nil {
		log.Errorf("Error generating Json from joined bot user #%d. Err: '%s'", messageNumber, err)
	} else {
		err := joiner.outputQueue.PublishData(data)

		if err != nil {
			log.Errorf("Error sending joined bot user #%d to output queue %s. Err: '%s'", messageNumber, joiner.outputQueue.Name, err)
		} else {
			log.Infof("Joined bot user #%d sent to output queue %s.", messageNumber, joiner.outputQueue.Name)
		}
	}

	wg.Done()
}

func (joiner *Joiner) Stop() {
	log.Infof("Closing Bot-Users Joiner connections.")
	joiner.connection.Close()
	joiner.channel.Close()
}
