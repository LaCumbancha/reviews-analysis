package common

import (
	"fmt"
	"sync"
	"strings"
	"encoding/json"
	"github.com/LaCumbancha/reviews-analysis/nodes/aggregators/weekday/rabbitmq"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/nodes/aggregators/weekday/logger"
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

func (calculator *Calculator) status(bulkNumber int) string {
	statusResponse := fmt.Sprintf("Status by bulk $%d: ", bulkNumber)

	for weekday, reviews := range calculator.data {
		statusResponse += strings.ToUpper(fmt.Sprintf("%s (%d) ; ", weekday, reviews))
	}

	return statusResponse[0:len(statusResponse)-3]
}

func (calculator *Calculator) Aggregate(bulkNumber int, rawWeekdayDataBulk string) {
	var weekdayDataList []rabbitmq.WeekdayData
	json.Unmarshal([]byte(rawWeekdayDataBulk), &weekdayDataList)

	for _, weekdayData := range weekdayDataList {

		calculator.mutex.Lock()
		if value, found := calculator.data[weekdayData.Weekday]; found {
			newAmount := value + 1
		    calculator.data[weekdayData.Weekday] = newAmount
		} else {
			calculator.data[weekdayData.Weekday] = 1
		}
		calculator.mutex.Unlock()

	}

	logb.Instance().Infof(calculator.status(bulkNumber), bulkNumber)
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
