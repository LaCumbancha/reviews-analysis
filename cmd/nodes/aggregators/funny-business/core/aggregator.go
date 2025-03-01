package core

import (
	"fmt"
	"sync"
	"encoding/json"
	"github.com/streadway/amqp"
	
	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
	utils "github.com/LaCumbancha/reviews-analysis/cmd/common/utils"
	proc "github.com/LaCumbancha/reviews-analysis/cmd/common/processing"
	props "github.com/LaCumbancha/reviews-analysis/cmd/common/properties"
	comms "github.com/LaCumbancha/reviews-analysis/cmd/common/communication"
	rabbit "github.com/LaCumbancha/reviews-analysis/cmd/common/middleware"
)

type AggregatorConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	WorkersPool 		int
	InputTopic			string
	FunbizFilters 		int
	FuncitJoiners		int
	OutputBulkSize		int
}

type Aggregator struct {
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	workersPool 		int
	calculator			*Calculator
	inputDirect 		*rabbit.RabbitInputDirect
	outputDirect 		*rabbit.RabbitOutputDirect
	outputPartitions	map[string]string
	endSignals			int
}

func NewAggregator(config AggregatorConfig) *Aggregator {
	connection, channel := rabbit.EstablishConnection(config.RabbitIp, config.RabbitPort)

	inputDirect := rabbit.NewRabbitInputDirect(channel, props.FunbizFilterOutput, config.InputTopic, "")
	outputDirect := rabbit.NewRabbitOutputDirect(channel, props.FunbizAggregatorOutput, comms.EndMessage(config.Instance))

	aggregator := &Aggregator {
		connection:			connection,
		channel:			channel,
		workersPool:		config.WorkersPool,
		calculator:			NewCalculator(config.OutputBulkSize),
		inputDirect:		inputDirect,
		outputDirect:		outputDirect,
		outputPartitions:	utils.GeneratePartitionMap(config.FuncitJoiners, PartitionableValues),
		endSignals:			config.FunbizFilters,
	}

	return aggregator
}

func (aggregator *Aggregator) Run() {
	log.Infof("Starting to listen for funny-business data.")
	innerChannel := make(chan amqp.Delivery)

	var wg sync.WaitGroup
	wg.Add(1)

	go proc.InitializeProcessingWorkers(aggregator.workersPool, innerChannel, aggregator.aggregateCallback, &wg)
	go proc.ProcessInputs(aggregator.inputDirect.ConsumeData(), innerChannel, aggregator.endSignals, &wg)
	
    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are received.
    wg.Wait()

    outputBulkNumber := 0
    for _, aggregatedData := range aggregator.calculator.RetrieveData() {
		outputBulkNumber++
    	logb.Instance().Infof(fmt.Sprintf("Aggregated bulk #%d generated.", outputBulkNumber), outputBulkNumber)
		aggregator.sendAggregatedData(outputBulkNumber, aggregatedData)
	}

    // Sending End-Message to consumers.
    for _, partition := range utils.GetMapDistinctValues(aggregator.outputPartitions) {
    	aggregator.outputDirect.PublishFinish(partition)
    }
}

func (aggregator *Aggregator) aggregateCallback(bulkNumber int, bulk string) {
	aggregator.calculator.Aggregate(bulkNumber, bulk)
}

func (aggregator *Aggregator) sendAggregatedData(bulkNumber int, aggregatedBulk []comms.FunnyBusinessData) {
	dataListByPartition := make(map[string][]comms.FunnyBusinessData)

	for _, data := range aggregatedBulk {
		partition := aggregator.outputPartitions[string(data.BusinessId[0])]

		if partition != "" {
			funbizDataListPartitioned := dataListByPartition[partition]

			if funbizDataListPartitioned != nil {
				dataListByPartition[partition] = append(funbizDataListPartitioned, data)
			} else {
				dataListByPartition[partition] = append(make([]comms.FunnyBusinessData, 0), data)
			}

		} else {
			log.Errorf("Couldn't calculate partition for business '%s'.", data.BusinessId)
		}
	}

	for partition, funbizDataListPartitioned := range dataListByPartition {
		outputData, err := json.Marshal(funbizDataListPartitioned)

		if err != nil {
			log.Errorf("Error generating Json from (%s). Err: '%s'", funbizDataListPartitioned, err)
		} else {

			err := aggregator.outputDirect.PublishData(outputData, partition)

			if err != nil {
				log.Errorf("Error sending bulk #%d to direct-exchange %s (partition %s). Err: '%s'", bulkNumber, aggregator.outputDirect.Exchange, partition, err)
			} else {
				logb.Instance().Infof(fmt.Sprintf("Aggregated bulk #%d sent to direct-exchange %s (partition %s).", bulkNumber, aggregator.outputDirect.Exchange, partition), bulkNumber)
			}	
		}
	}
}

func (aggregator *Aggregator) Stop() {
	log.Infof("Closing Funny-Business Aggregator connections.")
	aggregator.connection.Close()
	aggregator.channel.Close()
}
