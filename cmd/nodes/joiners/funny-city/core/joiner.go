package core

import (
	"fmt"
	"sync"
	"encoding/json"
	"github.com/streadway/amqp"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	utils "github.com/LaCumbancha/reviews-analysis/cmd/common/utils"
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
	FunbizAggregators 	int
	CitbizMappers		int
	FuncitAggregators	int
	OutputBulkSize		int
}

type Joiner struct {
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	calculator			*Calculator
	inputDirect1 		*rabbit.RabbitInputDirect
	inputDirect2 		*rabbit.RabbitInputDirect
	outputDirect 		*rabbit.RabbitOutputDirect
	outputPartitions	map[string]string
	endSignals1			int
	endSignals2			int
}

func NewJoiner(config JoinerConfig) *Joiner {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputDirect1 := rabbit.NewRabbitInputDirect(channel, props.FunbizAggregatorOutput, config.InputTopic, "")
	inputDirect2 := rabbit.NewRabbitInputDirect(channel, props.CitbizMapperOutput, config.InputTopic, "")
	outputDirect := rabbit.NewRabbitOutputDirect(channel, props.FuncitJoinerOutput, comms.EndMessage(config.Instance))

	joiner := &Joiner {
		connection:			connection,
		channel:			channel,
		calculator:			NewCalculator(config.OutputBulkSize),
		inputDirect1:		inputDirect1,
		inputDirect2:		inputDirect2,
		outputDirect:		outputDirect,
		outputPartitions:	utils.GeneratePartitionMap(config.FuncitAggregators, PartitionableValues),
		endSignals1:		config.FunbizAggregators,
		endSignals2:		config.CitbizMappers,
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
		log.Infof("Starting to listen for funny-business data.")

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
				logb.Instance().Infof(fmt.Sprintf("Funbiz data bulk #%d received.", bulkCounter), bulkCounter)

				inputWg.Add(1)
				go func(bulkNumber int, bulk string) {
					joiner.calculator.AddFunnyBusiness(bulkNumber, bulk)
					inputWg.Done()
				}(bulkCounter, messageBody)
			}
		}
	}()

	// Receiving messages from the city-business flow.
	inputWg.Add(1)
	go func() {
		log.Infof("Starting to listen for city-business data.")

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
				logb.Instance().Infof(fmt.Sprintf("Citbiz data bulk #%d received.", bulkCounter), bulkCounter * 5)

				inputWg.Add(1)
				go func(bulkNumber int, bulk string) {
					joiner.calculator.AddCityBusiness(bulkNumber, bulk)
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
    for _, partition := range utils.GetMapDistinctValues(joiner.outputPartitions) {
    	joiner.outputDirect.PublishFinish(partition)
    }
}

func (joiner *Joiner) fetchJoinMatches(joinWg *sync.WaitGroup) {
	messageCounter := 0
	joinMatches := joiner.calculator.RetrieveMatches()

	if len(joinMatches) == 0 {
    	log.Warnf("No join matches to send.")
    }

    for _, joinedData := range joinMatches {
    	messageCounter++

    	joinWg.Add(1)
    	go joiner.sendJoinedData(messageCounter, joinedData, joinWg)
	}
}

func (joiner *Joiner) sendJoinedData(bulkNumber int, joinedBulk []comms.FunnyCityData, wg *sync.WaitGroup) {
	dataListByPartition := make(map[string][]comms.FunnyCityData)

	for _, data := range joinedBulk {
		partition := joiner.outputPartitions[string(data.City[0])]

		if partition != "" {
			funcitDataListPartitioned := dataListByPartition[partition]

			if funcitDataListPartitioned != nil {
				dataListByPartition[partition] = append(funcitDataListPartitioned, data)
			} else {
				dataListByPartition[partition] = append(make([]comms.FunnyCityData, 0), data)
			}

		} else {
			log.Errorf("Couldn't calculate partition for city '%s'.", data.City)
		}
	}

	for partition, userDataListPartitioned := range dataListByPartition {
		outputData, err := json.Marshal(userDataListPartitioned)

		if err != nil {
			log.Errorf("Error generating Json from (%s). Err: '%s'", userDataListPartitioned, err)
		} else {

			err := joiner.outputDirect.PublishData(outputData, partition)

			if err != nil {
				log.Errorf("Error sending bulk #%d to direct-exchange %s (partition %s). Err: '%s'", bulkNumber, joiner.outputDirect.Exchange, partition, err)
			} else {
				logb.Instance().Infof(fmt.Sprintf("Bulk #%d sent to direct-exchange %s (partition %s).", bulkNumber, joiner.outputDirect.Exchange, partition), bulkNumber)
			}	
		}
	}

	wg.Done()
}

func (joiner *Joiner) Stop() {
	log.Infof("Closing Funny-City Joiner connections.")
	joiner.connection.Close()
	joiner.channel.Close()
}
