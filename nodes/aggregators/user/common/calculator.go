package common

import (
	"fmt"
	"sync"
	"encoding/json"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/aggregators/user/logging"
	"github.com/LaCumbancha/reviews-analysis/nodes/aggregators/user/rabbitmq"
)

type Calculator struct {
	data 			map[string]int
	mutex 			*sync.Mutex
}

func NewCalculator() *Calculator {
	calculator := &Calculator {
		data:		make(map[string]int),
		mutex:		&sync.Mutex{},
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

	logging.Infof(fmt.Sprintf("Status by bulk #%d: %d users stored.", bulkNumber, len(calculator.data)), bulkNumber)
}

func (calculator *Calculator) RetrieveData() []rabbitmq.UserData {
	var list []rabbitmq.UserData

	for userId, reviews := range calculator.data {
		log.Infof("User %s reviews aggregated: %d.", userId, reviews)
		aggregatedData := rabbitmq.UserData {
			UserId:		userId,
			Reviews:	reviews,
		}
		list = append(list, aggregatedData)
	}

	return list
}
