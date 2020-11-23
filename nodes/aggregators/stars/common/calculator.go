package common

import (
	"sync"
	"encoding/json"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/aggregators/stars/rabbitmq"
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
	var starsData rabbitmq.StarsData
	json.Unmarshal([]byte(rawData), &starsData)

	calculator.mutex.Lock()

	if value, found := calculator.data[starsData.UserId]; found {
		newAmount := value + 1
	    calculator.data[starsData.UserId] = newAmount
	    log.Infof("User %s 5-stars reviews incremented to %d.", starsData.UserId, newAmount)
	} else {
		calculator.data[starsData.UserId] = 1
		log.Infof("Initialized user %s 5-stars reviews at 1.", starsData.UserId)
	}

	calculator.mutex.Unlock()
}

func (calculator *Calculator) RetrieveData() []rabbitmq.UserData {
	var list []rabbitmq.UserData

	for userId, reviews := range calculator.data {
		log.Infof("User %s 5-stars reviews aggregated: %d.", userId, reviews)
		aggregatedData := rabbitmq.UserData {
			UserId:		userId,
			Reviews:	reviews,
		}
		list = append(list, aggregatedData)
	}

	return list
}
