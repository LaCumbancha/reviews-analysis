package common

import (
	"fmt"
	"sort"
	"sync"
	"encoding/json"
	"github.com/LaCumbancha/reviews-analysis/nodes/filters/funny-city/rabbitmq"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/nodes/filters/funny-city/logger"
)

type Calculator struct {
	data 			[]rabbitmq.FunnyCityData
	mutex 			*sync.Mutex
	topSize			int
}

func NewCalculator(topSize int) *Calculator {
	calculator := &Calculator {
		data:		[]rabbitmq.FunnyCityData{},
		mutex:		&sync.Mutex{},
		topSize:	topSize,
	}

	return calculator
}

func (calculator *Calculator) Save(bulkNumber int, rawFuncitDataList string) {
	var funcitDataList []rabbitmq.FunnyCityData
	json.Unmarshal([]byte(rawFuncitDataList), &funcitDataList)

	for _, funcitData := range funcitDataList {
		calculator.mutex.Lock()
		calculator.data = append(calculator.data, funcitData)
		calculator.mutex.Unlock()
	}

	logb.Instance().Infof(fmt.Sprintf("Status by bulk #%d: %d funny cities stored.", bulkNumber, len(calculator.data)), bulkNumber)
}

func (calculator *Calculator) RetrieveTopTen() []rabbitmq.FunnyCityData {
	sort.SliceStable(calculator.data, func(cityIdx1, cityIdx2 int) bool {
	    return calculator.data[cityIdx1].Funny > calculator.data[cityIdx2].Funny
	})

	funnyCities := len(calculator.data)
	if (funnyCities > calculator.topSize) {
		log.Infof("%d cities where discarded due to not being funny enoguh.", funnyCities - calculator.topSize)
		return calculator.data[0:calculator.topSize]
	} else {
		log.Infof("They where just %d cities with a funniness higher than 0!", funnyCities)
		return calculator.data[0:funnyCities]
	}
}
