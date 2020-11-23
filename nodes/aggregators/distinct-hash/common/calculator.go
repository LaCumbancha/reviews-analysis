package common

import (
	"sync"
	"encoding/json"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/aggregators/distinct-hash/rabbitmq"
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
	var hashedData rabbitmq.HashedTextData
	json.Unmarshal([]byte(rawData), &hashedData)

	calculator.mutex.Lock()

	if value, found := calculator.data[hashedData.UserId]; found {
		newValue := value + 1
	    calculator.data[hashedData.UserId] = newValue
	    log.Infof("User %s repeated hash %s %d times.", hashedData.UserId, hashedData.HashedText, newValue)
	} else {
		calculator.data[hashedData.UserId] = 1
		log.Infof("User %s registered new text: %s.", hashedData.UserId, hashedData.HashedText)
	}

	calculator.mutex.Unlock()
}

func (calculator *Calculator) RetrieveData() []rabbitmq.DistinctHashesData {
	var list []rabbitmq.DistinctHashesData

	for userId, distinctHashes := range calculator.data {
		log.Infof("User %s commented %s texts differents.", userId, distinctHashes)
		aggregatedData := rabbitmq.DistinctHashesData {
			UserId:			userId,
			Distinct:		distinctHashes,
		}
		list = append(list, aggregatedData)
	}

	return list
}
