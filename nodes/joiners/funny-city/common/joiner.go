package common

import (
	"fmt"
	"sync"
	"encoding/json"
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/joiners/funny-city/rabbitmq"
)

type JoinerConfig struct {
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
	outputDirect := rabbitmq.NewRabbitOutputDirect(rabbitmq.OUTPUT_EXCHANGE_NAME, config.FuncitAggregators, ch)
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

	var endSignals1Received = 0
	var endSignals2Received = 0

	var inputWg sync.WaitGroup

	// Receiving messages from the funny-business flow.
	inputWg.Add(1)
	go func() {
		log.Infof("Starting to listen for funny-business data.")
		for message := range joiner.inputDirect1.ConsumeData() {
			messageBody := string(message.Body)

			if messageBody == rabbitmq.END_MESSAGE {
				// Waiting for the total needed End-Signals to send the own End-Message.
				endSignals1Mutex.Lock()
				endSignals1Received++
				endSignals1Mutex.Unlock()
				log.Infof("End-Message #%d received from Funny-Business flow.", endSignals1Received)

				if (endSignals1Received == joiner.endSignals1) {
					log.Infof("All End-Messages were received from the Funny-Business flow.")
					inputWg.Done()
				}
				
				//rabbitmq.AckMessage(&message, rabbitmq.END_MESSAGE)
			} else {
				log.Infof("Data '%s' received.", messageBody)

				inputWg.Add(1)
				go func() {
					joiner.calculator.AddFunnyBusiness(messageBody)
					//rabbitmq.AckMessage(&message, utils.GetReviewId(review))
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

			if messageBody == rabbitmq.END_MESSAGE {
				// Waiting for the total needed End-Signals to send the own End-Message.
				endSignals2Mutex.Lock()
				endSignals2Received++
				endSignals2Mutex.Unlock()
				log.Infof("End-Message #%d received from City-Business flow.", endSignals2Received)

				if (endSignals2Received == joiner.endSignals2) {
					log.Infof("All End-Messages were received from the City-Business flow.")
					inputWg.Done()
				}
				
				//rabbitmq.AckMessage(&message, rabbitmq.END_MESSAGE)
			} else {
				log.Infof("Data '%s' received.", messageBody)

				inputWg.Add(1)
				go func() {
					joiner.calculator.AddCityBusiness(messageBody)
					//rabbitmq.AckMessage(&message, utils.GetReviewId(review))
					inputWg.Done()
				}()
			}
		}
	}()

	var lastCheckMutex = &sync.Mutex{}
	lastCheck := false

	// Processing joins matches concurrently.
	var joinWg sync.WaitGroup
	joinWg.Add(1)
	go func() {
		for {
    		newJoin := false
    		lastCheckMutex.Lock()
    		lastJoinRound := lastCheck
    		lastCheckMutex.Unlock()

    		joinedData := joiner.calculator.RetrieveData()
    		if len(joinedData) > 0 {
    			newJoin = true
    		} else {
    			log.Debugf("No new join match to send in this round. Finishing process.")
    		}

    		for _, joinedData := range joiner.calculator.RetrieveData() {
    			joinWg.Add(1)
    			log.Infof("Starting sending joined funny data from city %s.", joinedData.City)
				go joiner.sendJoinedData(joinedData, &joinWg)
			}

    		if lastJoinRound && !newJoin {
    			log.Infof("No new join match to send at all. Finishing process.")
    			joinWg.Done()
    			break
    		}
    	}
	}()
    

    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are received.
    inputWg.Wait()

    lastCheckMutex.Lock()
    lastCheck = true
    lastCheckMutex.Unlock()

    // Using WaitGroups to avoid closing the RabbitMQ connection before all joins are processed and sent.
    joinWg.Wait()

    // Sending End-Message to consumers.
    joiner.outputDirect.PublishFinish()
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
