package common

import (
	"fmt"
	"sync"
	"encoding/json"
	"github.com/LaCumbancha/reviews-analysis/nodes/aggregators/distinct-hash/rabbitmq"

	logb "github.com/LaCumbancha/reviews-analysis/nodes/aggregators/distinct-hash/logger"
)

type Calculator struct {
	data 			map[string]int
	mutex 			*sync.Mutex
	bulkSize		int
}

func NewCalculator(bulkSize int) *Calculator {
	calculator := &Calculator {
		data:		make(map[string]int),
		mutex:		&sync.Mutex{},
		bulkSize:	bulkSize,
	}

	return calculator
}

func (calculator *Calculator) Aggregate(bulkNumber int, rawHashedDataBulk string) {
	var hashedDataList []rabbitmq.HashedTextData
	json.Unmarshal([]byte(rawHashedDataBulk), &hashedDataList)

	for _, hashedData := range hashedDataList {

		calculator.mutex.Lock()
		if value, found := calculator.data[hashedData.UserId]; found {
			newValue := value + 1
	    	calculator.data[hashedData.UserId] = newValue
		} else {
			calculator.data[hashedData.UserId] = 1
		}
		calculator.mutex.Unlock()

	}

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d: %d users stored.", bulkNumber, len(calculator.data)), bulkNumber)
}

func (calculator *Calculator) RetrieveData() [][]rabbitmq.DistinctHashesData {
	bulk := make([]rabbitmq.DistinctHashesData, 0)
	bulkedList := make([][]rabbitmq.DistinctHashesData, 0)

	actualBulk := 0
	for userId, distinctHashes := range calculator.data {
		actualBulk++
		aggregatedData := rabbitmq.DistinctHashesData { UserId: userId, Distinct: distinctHashes }
		bulk = append(bulk, aggregatedData)

		if actualBulk == calculator.bulkSize {
			bulkedList = append(bulkedList, bulk)
			bulk = make([]rabbitmq.DistinctHashesData, 0)
			actualBulk = 0
		}
	}

	if len(bulk) != 0 {
		bulkedList = append(bulkedList, bulk)
	}

	return bulkedList
}
