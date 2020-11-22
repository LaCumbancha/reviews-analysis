package common

import (
	"sync"
	"encoding/json"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/aggregators/weekday/rabbitmq"
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
	var weekdayReview rabbitmq.WeekdayData
	json.Unmarshal([]byte(rawData), &weekdayReview)

	calculator.mutex.Lock()

	if value, found := calculator.data[weekdayReview.Weekday]; found {
		newAmount := value + 1
	    calculator.data[weekdayReview.Weekday] = newAmount
	    log.Infof("%s reviews incremented to %d.", weekdayReview.Weekday, newAmount)
	} else {
		calculator.data[weekdayReview.Weekday] = 1
		log.Infof("Initialized %s reviews at 1.", weekdayReview.Weekday)
	}

	calculator.mutex.Unlock()
}

func (calculator *Calculator) RetrieveData() []rabbitmq.WeekdayData {
	var list []rabbitmq.WeekdayData

	for weekday, reviews := range calculator.data {
		log.Infof("%s reviews aggregated: %d.", weekday, reviews)
		aggregatedData := rabbitmq.WeekdayData {
			Weekday:		weekday,
			Reviews:		reviews,
		}
		list = append(list, aggregatedData)
	}

	return list
}
