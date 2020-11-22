package common

import (
	"sync"
	"encoding/json"
	log "github.com/sirupsen/logrus"

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

func (calculator *Calculator) Aggregate(rawData string) {
	var userData rabbitmq.UserData
	json.Unmarshal([]byte(rawData), &userData)

	calculator.mutex.Lock()

	if value, found := calculator.data[userData.UserId]; found {
		newAmount := value + 1
	    calculator.data[userData.UserId] = newAmount
	    log.Infof("User %s reviews incremented to %d.", userData.UserId, newAmount)
	} else {
		calculator.data[userData.UserId] = 1
		log.Infof("Initialized user %s reviews at 1.", userData.UserId)
	}

	calculator.mutex.Unlock()
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
