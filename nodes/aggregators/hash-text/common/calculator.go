package common

import (
	"sync"
	"encoding/json"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/aggregators/hash-text/rabbitmq"
)

type Calculator struct {
	data 			map[string]map[string]int
	mutex 			*sync.Mutex
}

func NewCalculator() *Calculator {
	calculator := &Calculator {
		data:		make(map[string]map[string]int),
		mutex:		&sync.Mutex{},
	}

	return calculator
}

func (calculator *Calculator) Aggregate(rawData string) {
	var hashedData rabbitmq.HashedTextData
	json.Unmarshal([]byte(rawData), &hashedData)

	calculator.mutex.Lock()

	if userTexts, found := calculator.data[hashedData.UserId]; found {
		if _, found := userTexts[hashedData.HashedText]; !found { 
			userTexts[hashedData.HashedText] = 1
			log.Infof("Hashed text %s added to user %s.", hashedData.HashedText, hashedData.UserId)
		}
	} else {
		calculator.data[hashedData.UserId] = make(map[string]int)
		calculator.data[hashedData.UserId][hashedData.HashedText] = 1
		log.Infof("Initialized user %s hashed text as %s.", hashedData.UserId, hashedData.HashedText)
	}

	calculator.mutex.Unlock()
}

func (calculator *Calculator) RetrieveData() []rabbitmq.HashedTextData {
	var list []rabbitmq.HashedTextData

	for userId, hashes := range calculator.data {
		for hash, _ := range hashes {
			log.Infof("User %s distinct hashed text: %s.", userId, hash)
			aggregatedData := rabbitmq.HashedTextData {
				UserId:		userId,
				HashedText:	hash,
			}
			list = append(list, aggregatedData)
		}
	}

	return list
}
