package common

import (
	"sort"
	"sync"
	"encoding/json"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/filters/funny-city/rabbitmq"
)

type Calculator struct {
	data 			[]rabbitmq.FunnyCityData
	mutex 			*sync.Mutex
}

func NewCalculator() *Calculator {
	calculator := &Calculator {
		data:		[]rabbitmq.FunnyCityData{},
		mutex:		&sync.Mutex{},
	}

	return calculator
}

func (calculator *Calculator) Save(rawData string) {
	var funnyCity rabbitmq.FunnyCityData
	json.Unmarshal([]byte(rawData), &funnyCity)

	calculator.mutex.Lock()
	calculator.data = append(calculator.data, funnyCity)
	calculator.mutex.Unlock()

	log.Infof("City %s saved with funniness at %d.", funnyCity.City, funnyCity.Funny)
}

func (calculator *Calculator) RetrieveTopTen() []rabbitmq.FunnyCityData {
	sort.SliceStable(calculator.data, func(cityIdx1, cityIdx2 int) bool {
	    return calculator.data[cityIdx1].Funny > calculator.data[cityIdx2].Funny
	})

	funnyCities := len(calculator.data)
	if (funnyCities > 10) {
		log.Infof("%d cities where discarded due to not being funny enoguh.", funnyCities - 10)
		return calculator.data[0:9]
	} else {
		log.Infof("They where just %d cities with a funniness higher than 0!", funnyCities)
		return calculator.data[0:funnyCities]
	}
}
