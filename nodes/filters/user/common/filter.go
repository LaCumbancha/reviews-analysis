package common

import (
	"fmt"
	"sync"
	"encoding/json"
	"github.com/streadway/amqp"
	"github.com/LaCumbancha/reviews-analysis/nodes/filters/user/rabbitmq"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/nodes/filters/user/logger"
)

type FilterConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	MinReviews			int
	UserAggregators		int
	StarsJoiners		int
}

type Filter struct {
	connection 		*amqp.Connection
	channel 		*amqp.Channel
	minReviews 		int
	inputQueue 		*rabbitmq.RabbitInputQueue
	outputQueue 	*rabbitmq.RabbitOutputQueue
	outputDirect	*rabbitmq.RabbitOutputDirect
	endSignals		int
}

func NewFilter(config FilterConfig) *Filter {
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

	inputQueue := rabbitmq.NewRabbitInputQueue(rabbitmq.INPUT_QUEUE_NAME, ch)
	outputQueue := rabbitmq.NewRabbitOutputQueue(rabbitmq.OUTPUT_QUEUE_NAME, config.Instance, ch)
	outputDirect := rabbitmq.NewRabbitOutputDirect(rabbitmq.OUTPUT_EXCHANGE_NAME, config.Instance, config.StarsJoiners, ch)
	filter := &Filter {
		connection:		conn,
		channel:		ch,
		minReviews:		config.MinReviews,
		inputQueue:		inputQueue,
		outputQueue:	outputQueue,
		outputDirect:	outputDirect,
		endSignals:		config.UserAggregators,
	}

	return filter
}

func (filter *Filter) Run() {
	log.Infof("Starting to listen for user reviews data.")

	var endSignalsMutex = &sync.Mutex{}
	var endSignals = make(map[string]int)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		bulkCounter := 0
		for message := range filter.inputQueue.ConsumeData() {
			messageBody := string(message.Body)

			if rabbitmq.IsEndMessage(messageBody) {
				filter.processEndSignal(messageBody, endSignals, endSignalsMutex, &wg)
			} else {
				bulkCounter++
				logb.Instance().Infof(fmt.Sprintf("User data bulk #%d received.", bulkCounter), bulkCounter)

				wg.Add(1)
				go func(bulkNumber int) {
					filter.filterActiveUsers(bulkNumber, messageBody)
					wg.Done()
				}(bulkCounter)
			}
		}
	}()
	
    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are sent.
    wg.Wait()

    // Publishing end messages.
    filter.outputQueue.PublishFinish()
    filter.outputDirect.PublishFinish()
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

func (filter *Filter) filterActiveUsers(bulkNumber int, rawUserDataBulk string) {
	var userDataList []rabbitmq.UserData
	var filteredUserDataList []rabbitmq.UserData
	json.Unmarshal([]byte(rawUserDataBulk), &userDataList)

	for _, userData := range userDataList {
		if (userData.Reviews >= filter.minReviews) {
			filteredUserDataList = append(filteredUserDataList, userData)
			filter.outputQueue.PublishData(userData)
		}
	}

	filter.outputDirect.PublishData(bulkNumber, filteredUserDataList)
}

func (filter *Filter) Stop() {
	log.Infof("Closing User Filter connections.")
	filter.connection.Close()
	filter.channel.Close()
}
