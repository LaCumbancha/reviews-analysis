package common

import (
	"sync"
	"encoding/json"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/aggregators/funny-city/rabbitmq"
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
	var funnyCity rabbitmq.FunnyCityData
	json.Unmarshal([]byte(rawData), &funnyCity)

	calculator.mutex.Lock()

	if value, found := calculator.data[funnyCity.City]; found {
		newFunny := value + funnyCity.Funny
	    calculator.data[funnyCity.City] = newFunny
	    log.Infof("%s funniness incremented to %d.", funnyCity.City, newFunny)
	} else {
		calculator.data[funnyCity.City] = funnyCity.Funny
		log.Infof("Initialized %s funniness at %d.", funnyCity.City, funnyCity.Funny)
	}

	calculator.mutex.Unlock()
}

func (calculator *Calculator) RetrieveData() []rabbitmq.FunnyCityData {
	var list []rabbitmq.FunnyCityData

	for city, funny := range calculator.data {
		log.Infof("%s funniness aggregated: %d.", city, funny)
		aggregatedData := rabbitmq.FunnyCityData {
			City:		city,
			Funny:			funny,
		}
		list = append(list, aggregatedData)
	}

	return list
}
