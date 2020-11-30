package common

import (
	"fmt"
	"sync"
	"encoding/json"
	"github.com/LaCumbancha/reviews-analysis/nodes/aggregators/user/rabbitmq"

	logb "github.com/LaCumbancha/reviews-analysis/nodes/aggregators/user/logger"
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

func (calculator *Calculator) Aggregate(bulkNumber int, rawUserDataBulk string) {
	var userDataList []rabbitmq.UserData
	json.Unmarshal([]byte(rawUserDataBulk), &userDataList)

	for _, userData := range userDataList {

		calculator.mutex.Lock()
		if value, found := calculator.data[userData.UserId]; found {
			newAmount := value + 1
		    calculator.data[userData.UserId] = newAmount
		} else {
			calculator.data[userData.UserId] = 1
		}
		calculator.mutex.Unlock()

	}

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d: %d users stored.", bulkNumber, len(calculator.data)), bulkNumber)
}

func (calculator *Calculator) RetrieveData() [][]rabbitmq.UserData {
	bulk := make([]rabbitmq.UserData, 0)
	bulkedList := make([][]rabbitmq.UserData, 0)

	actualBulk := 0
	for userId, reviews := range calculator.data {
		actualBulk++
		aggregatedData := rabbitmq.UserData { UserId: userId, Reviews: reviews }
		bulk = append(bulk, aggregatedData)

		if actualBulk == calculator.bulkSize {
			bulkedList = append(bulkedList, bulk)
			bulk = make([]rabbitmq.UserData, 0)
			actualBulk = 0
		}

	}

	if len(bulk) != 0 {
		bulkedList = append(bulkedList, bulk)
	}

	return bulkedList
}
