package common

import (
	"sync"
	"encoding/json"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/aggregators/funny-business/rabbitmq"
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
	var funnyBusiness rabbitmq.FunnyBusinessData
	json.Unmarshal([]byte(rawData), &funnyBusiness)

	calculator.mutex.Lock()

	if value, found := calculator.data[funnyBusiness.BusinessId]; found {
		newFunny := value + funnyBusiness.Funny
	    calculator.data[funnyBusiness.BusinessId] = newFunny
	    log.Infof("Business %s funniness incremented to %d.", funnyBusiness.BusinessId, newFunny)
	} else {
		calculator.data[funnyBusiness.BusinessId] = funnyBusiness.Funny
		log.Infof("Initialized business %s funniness at %d.", funnyBusiness.BusinessId, funnyBusiness.Funny)
	}

	calculator.mutex.Unlock()
}

func (calculator *Calculator) RetrieveData() []rabbitmq.FunnyBusinessData {
	var list []rabbitmq.FunnyBusinessData

	for businessId, funny := range calculator.data {
		log.Infof("Business %s funniness aggregated: %d.", businessId, funny)
		aggregatedData := rabbitmq.FunnyBusinessData {
			BusinessId:		businessId,
			Funny:			funny,
		}
		list = append(list, aggregatedData)
	}

	return list
}
