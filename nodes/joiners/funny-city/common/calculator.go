package common

import (
	"fmt"
	"sync"
	"encoding/json"
	"github.com/LaCumbancha/reviews-analysis/nodes/joiners/funny-city/rabbitmq"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/nodes/joiners/funny-city/logger"
)

type Calculator struct {
	data1 			map[string]int
	data2 			map[string]string
	mutex1 			*sync.Mutex
	mutex2 			*sync.Mutex
}

func NewCalculator() *Calculator {
	calculator := &Calculator {
		data1:		make(map[string]int),
		data2:		make(map[string]string),
		mutex1:		&sync.Mutex{},
		mutex2:		&sync.Mutex{},
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

func (calculator *Calculator) RetrieveMatches() []rabbitmq.FunnyCityData {
	var list []rabbitmq.FunnyCityData

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

			log.Infof("City join match for business %s (from city %s with funniness %d).", businessId, city, funny)
			list = append(list, joinedData)
		} else {
			calculator.mutex2.Unlock()
			log.Tracef("Still no city join match for businessId %s.", businessId)
		}

		calculator.mutex1.Lock()
	}

	calculator.mutex1.Unlock()
	return list
}
