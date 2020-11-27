package common

import (
	"fmt"
	"sync"
	"encoding/json"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/aggregators/hash-text/logging"
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

func (calculator *Calculator) Aggregate(bulkNumber int, rawHashedDataBulk string) {
	var hashedDataList []rabbitmq.HashedTextData
	json.Unmarshal([]byte(rawHashedDataBulk), &hashedDataList)

	for _, hashedData := range hashedDataList {

		calculator.mutex.Lock()
		if userTexts, found := calculator.data[hashedData.UserId]; found {
			if _, found := userTexts[hashedData.HashedText]; !found { 
				userTexts[hashedData.HashedText] = 1
			}
		} else {
			calculator.data[hashedData.UserId] = make(map[string]int)
			calculator.data[hashedData.UserId][hashedData.HashedText] = 1
		}
		calculator.mutex.Unlock()

	}

	logging.Infof(fmt.Sprintf("Status by bulk #%d: %d users stored.", bulkNumber, len(calculator.data)), bulkNumber)
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
