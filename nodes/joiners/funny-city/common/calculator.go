package common

import (
	"fmt"
	"sync"
	"encoding/json"
	"github.com/LaCumbancha/reviews-analysis/nodes/joiners/funny-city/rabbitmq"

	logb "github.com/LaCumbancha/reviews-analysis/nodes/joiners/funny-city/logger"
)

type Calculator struct {
	data1 			map[string]int
	data2 			map[string]string
	mutex1 			*sync.Mutex
	mutex2 			*sync.Mutex
	maxBulkSize		int
}

func NewCalculator(bulkSize int) *Calculator {
	calculator := &Calculator {
		data1:			make(map[string]int),
		data2:			make(map[string]string),
		mutex1:			&sync.Mutex{},
		mutex2:			&sync.Mutex{},
		maxBulkSize:	bulkSize,
	}

	return calculator
}

func (calculator *Calculator) AddFunnyBusiness(bulkNumber int, rawFunbizDataBulk string) {
	var funbizDataList []rabbitmq.FunnyBusinessData
	json.Unmarshal([]byte(rawFunbizDataBulk), &funbizDataList)

	for _, funbizData := range funbizDataList {
		calculator.mutex1.Lock()
		calculator.data1[funbizData.BusinessId] = funbizData.Funny
		calculator.mutex1.Unlock()
	}

	logb.Instance().Infof(fmt.Sprintf("Funbiz data bulk #%d stored in Joiner", bulkNumber), bulkNumber)
}

func (calculator *Calculator) AddCityBusiness(bulkNumber int, rawCitbizDataBulk string) {
	var citbizDataList []rabbitmq.CityBusinessData
	json.Unmarshal([]byte(rawCitbizDataBulk), &citbizDataList)

	for _, citbizData := range citbizDataList {
		calculator.mutex2.Lock()
		calculator.data2[citbizData.BusinessId] = citbizData.City
		calculator.mutex2.Unlock()
	}

	logb.Instance().Infof(fmt.Sprintf("Citbiz data bulk #%d stored in Joiner", bulkNumber), bulkNumber)
}

func (calculator *Calculator) RetrieveMatches() [][]rabbitmq.FunnyCityData {
	var bulkedList [][]rabbitmq.FunnyCityData
	var bulk []rabbitmq.FunnyCityData

	actualBulk := 0
	calculator.mutex1.Lock()
	for businessId, funny := range calculator.data1 {
		calculator.mutex1.Unlock()

		calculator.mutex2.Lock()
		if city, found := calculator.data2[businessId]; found {
			calculator.mutex2.Unlock()
			joinedData := rabbitmq.FunnyCityData {
				City:		city,
				Funny:		funny,
			}

			calculator.mutex1.Lock()
			delete(calculator.data1, businessId);
			calculator.mutex1.Unlock()

			bulk = append(bulk, joinedData)
			actualBulk++

			if actualBulk == calculator.maxBulkSize {
				bulkedList = append(bulkedList, bulk)
				bulk = make([]rabbitmq.FunnyCityData, 0)
				actualBulk = 0
			}

		} else {
			calculator.mutex2.Unlock()
		}

		calculator.mutex1.Lock()
	}

	calculator.mutex1.Unlock()

	if len(bulk) != 0 {
		bulkedList = append(bulkedList, bulk)
	}

	return bulkedList
}
