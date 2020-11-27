package common

import (
	"fmt"
	"sync"
	"strings"
	"encoding/json"
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/mappers/city-business/rabbitmq"
)

type MapperConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	BusinessesInputs	int
	FuncitJoiners 		int
}

type Mapper struct {
	connection 		*amqp.Connection
	channel 		*amqp.Channel
	inputQueue 		*rabbitmq.RabbitInputQueue
	outputDirect 	*rabbitmq.RabbitOutputDirect
	endSignals		int
}

func NewMapper(config MapperConfig) *Mapper {
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
	outputDirect := rabbitmq.NewRabbitOutputDirect(rabbitmq.OUTPUT_EXCHANGE_NAME, config.Instance, config.FuncitJoiners, ch)
	mapper := &Mapper {
		connection:		conn,
		channel:		ch,
		inputQueue:		inputQueue,
		outputDirect:	outputDirect,
		endSignals:		config.BusinessesInputs,
	}

	return mapper
}

func (mapper *Mapper) Run() {
	log.Infof("Starting to listen for business.")

	var endSignalsMutex = &sync.Mutex{}
	var endSignals = make(map[string]int)

	bulkMutex := &sync.Mutex{}
	bulkNumber := 0

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for message := range mapper.inputQueue.ConsumeBusiness() {
			messageBody := string(message.Body)

			if rabbitmq.IsEndMessage(messageBody) {
				mapper.processEndSignal(messageBody, endSignals, endSignalsMutex, &wg)
			} else {
				var innerBulk int

				bulkMutex.Lock()
				innerBulk = bulkNumber
				bulkNumber++
				bulkMutex.Unlock()

				log.Infof("Businesses bulk #%d received.", innerBulk)

				wg.Add(1)
				go func() {
					mapper.processBusinessesBulk(messageBody)
					wg.Done()
				}()
			}
		}
	}()
	
    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are sent.
    wg.Wait()

    // Publishing end messages.
    mapper.outputDirect.PublishFinish()
}

func (mapper *Mapper) processEndSignal(newMessage string, endSignals map[string]int, mutex *sync.Mutex, wg *sync.WaitGroup) {
	mutex.Lock()
	endSignals[newMessage] = endSignals[newMessage] + 1
	newSignal := endSignals[newMessage] == 1
	signalsReceived := len(endSignals)
	mutex.Unlock()

	log.Infof("End-Message #%d received.", signalsReceived)

	// Waiting for the total needed End-Signals to send the own End-Message.
	if (signalsReceived == mapper.endSignals) && newSignal {
		log.Infof("All End-Messages were received.")
		wg.Done()
	}
}

func (mapper *Mapper) processBusinessesBulk(rawBusinessesBulk string) {
	var business rabbitmq.FullBusiness

	rawBusinesses := strings.Split(rawBusinessesBulk, "\n")
	for _, rawBusiness := range rawBusinesses {
		json.Unmarshal([]byte(rawBusiness), &business)
	
		cityBusiness := &rabbitmq.CityBusinessData {
			BusinessId:		business.BusinessId,
			City:			fmt.Sprintf("%s (%s)", business.City, business.State),
		}

		data, err := json.Marshal(cityBusiness)
		if err != nil {
			log.Errorf("Error generating Json from (%s). Err: '%s'", cityBusiness, err)
		} else {
			mapper.outputDirect.PublishData(data, cityBusiness.BusinessId)
		}
    }
}

func (mapper *Mapper) Stop() {
	log.Infof("Closing City-Business Mapper connections.")
	mapper.connection.Close()
	mapper.channel.Close()
}
