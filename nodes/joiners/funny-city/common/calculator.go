package common

import (
	"sync"
	"encoding/json"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/joiners/funny-city/rabbitmq"
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

func (calculator *Calculator) AddFunnyBusiness(rawData string) {
	var funnyBusiness rabbitmq.FunnyBusinessData
	json.Unmarshal([]byte(rawData), &funnyBusiness)

	calculator.mutex1.Lock()

	calculator.data1[funnyBusiness.BusinessId] = funnyBusiness.Funny
	log.Infof("Business %s funniness stored at %d.", funnyBusiness.BusinessId, funnyBusiness.Funny)

	calculator.mutex1.Unlock()
}

func (calculator *Calculator) AddCityBusiness(rawData string) {
	var cityBusiness rabbitmq.CityBusinessData
	json.Unmarshal([]byte(rawData), &cityBusiness)

	calculator.mutex2.Lock()

	calculator.data2[cityBusiness.BusinessId] = cityBusiness.City
	log.Infof("Business %s city stored as %s.", cityBusiness.BusinessId, cityBusiness.City)

	calculator.mutex2.Unlock()
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
