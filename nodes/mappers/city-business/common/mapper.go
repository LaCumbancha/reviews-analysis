package common

import (
	"fmt"
	"sync"
	"encoding/json"
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/mappers/city-business/utils"
	"github.com/LaCumbancha/reviews-analysis/nodes/mappers/city-business/rabbitmq"
)

type MapperConfig struct {
	RabbitIp			string
	RabbitPort			string
	FuncitJoiners 		int
}

type Mapper struct {
	connection 		*amqp.Connection
	channel 		*amqp.Channel
	inputQueue 		*rabbitmq.RabbitInputQueue
	outputDirect 	*rabbitmq.RabbitOutputDirect
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
	outputDirect := rabbitmq.NewRabbitOutputDirect(rabbitmq.OUTPUT_EXCHANGE_NAME, config.FuncitJoiners, ch)
	mapper := &Mapper {
		connection:		conn,
		channel:		ch,
		inputQueue:		inputQueue,
		outputDirect:	outputDirect,
	}

	return mapper
}

func (mapper *Mapper) Run() {
	log.Infof("Starting to listen for business.")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for message := range mapper.inputQueue.ConsumeBusiness() {
			messageBody := string(message.Body)

			if messageBody == rabbitmq.END_MESSAGE {
				log.Infof("End-Message received.")
				wg.Done()
				//rabbitmq.AckMessage(&message, rabbitmq.END_MESSAGE)
			} else {
				business := messageBody
				log.Infof("Business %s received.", utils.GetBusinessId(business))

				wg.Add(1)
				go func() {
					mapper.processBusiness(business)
					//rabbitmq.AckMessage(&message, utils.GetBusinessId(business))
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

func (mapper *Mapper) processBusiness(rawBusiness string) {
	var fullBusiness rabbitmq.FullBusiness
	json.Unmarshal([]byte(rawBusiness), &fullBusiness)

	mappedBusiness := &rabbitmq.CityBusinessData {
		BusinessId:		fullBusiness.BusinessId,
		City:			fmt.Sprintf("%s (%s)", fullBusiness.City, fullBusiness.State),
	}

	data, err := json.Marshal(mappedBusiness)
	if err != nil {
		log.Errorf("Error generating Json from (%s). Err: '%s'", mappedBusiness, err)
	} else {
		mapper.outputDirect.PublishData(data, fullBusiness.BusinessId)
	}
}

func (mapper *Mapper) Stop() {
	log.Infof("Closing City-Business Mapper connections.")
	mapper.connection.Close()
	mapper.channel.Close()
}
