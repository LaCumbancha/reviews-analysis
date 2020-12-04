package core

import (
	"fmt"
	"sync"
	"encoding/json"
	"github.com/streadway/amqp"
	"github.com/LaCumbancha/reviews-analysis/cmd/nodes/filters/user/rabbitmq"

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
	MinReviews			int
	UserAggregators		int
	StarsJoiners		int
}

type Filter struct {
	connection 		*amqp.Connection
	channel 		*amqp.Channel
	minReviews 		int
	inputQueue 		*rabbit.RabbitInputQueue
	outputQueue 	*rabbit.RabbitOutputQueue
	outputDirect	*rabbitmq.RabbitOutputDirect
	endSignals		int
}

func NewFilter(config FilterConfig) *Filter {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputQueue := rabbit.NewRabbitInputQueue(channel, props.UserAggregatorOutput)
	outputQueue := rabbit.NewRabbitOutputQueue(channel, props.UserFilterOutput, comms.EndMessage(config.Instance), comms.EndSignals(1))
	outputDirect := rabbitmq.NewRabbitOutputDirect(props.BestUsersFilterOutput, config.Instance, config.StarsJoiners, channel)

	filter := &Filter {
		connection:		connection,
		channel:		channel,
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

			if comms.IsEndMessage(messageBody) {
				filter.processEndSignal(messageBody, endSignals, endSignalsMutex, &wg)
			} else {
				bulkCounter++
				logb.Instance().Infof(fmt.Sprintf("User data bulk #%d received.", bulkCounter), bulkCounter)

				wg.Add(1)
				go func(bulkNumber int, bulk string) {
					filteredBulk := filter.filterData(bulkNumber, bulk)
					filter.sendFilteredData(bulkNumber, filteredBulk, &wg)
				}(bulkCounter, messageBody)
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

func (filter *Filter) filterData(bulkNumber int, rawUserDataBulk string) []comms.UserData {
	var userDataList []comms.UserData
	var filteredUserDataList []comms.UserData
	json.Unmarshal([]byte(rawUserDataBulk), &userDataList)

	for _, userData := range userDataList {
		if (userData.Reviews >= filter.minReviews) {
			filteredUserDataList = append(filteredUserDataList, userData)
		}
	}

	return filteredUserDataList
}

func (filter *Filter) sendFilteredData(bulkNumber int, filteredBulk []comms.UserData, wg *sync.WaitGroup) {
	data, err := json.Marshal(filteredBulk)
	if err != nil {
		log.Errorf("Error generating Json from filtered bulk #%d. Err: '%s'", bulkNumber, err)
	} else {
		err := filter.outputQueue.PublishData(data)

		if err != nil {
			log.Errorf("Error sending filtered bulk #%d to output queue %s. Err: '%s'", bulkNumber, filter.outputQueue.Name, err)
		} else {
			logb.Instance().Infof(fmt.Sprintf("Filtered bulk #%d sent to output queue %s.", bulkNumber, filter.outputQueue.Name), bulkNumber)
		}
	}

	filter.outputDirect.PublishData(bulkNumber, filteredBulk)
	wg.Done()
}

func (filter *Filter) Stop() {
	log.Infof("Closing User Filter connections.")
	filter.connection.Close()
	filter.channel.Close()
}
