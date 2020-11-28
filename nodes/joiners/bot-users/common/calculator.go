package common

import (
	"sync"
	"encoding/json"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/joiners/bot-users/rabbitmq"
)

type Calculator struct {
	data1 			map[string]int
	data2 			map[string]int
	mutex1 			*sync.Mutex
	mutex2 			*sync.Mutex
}

func NewCalculator() *Calculator {
	calculator := &Calculator {
		data1:		make(map[string]int),
		data2:		make(map[string]int),
		mutex1:		&sync.Mutex{},
		mutex2:		&sync.Mutex{},
	}

	return calculator
}

func (calculator *Calculator) AddBotUser(rawData string) {
	var userData rabbitmq.UserData
	json.Unmarshal([]byte(rawData), &userData)

	calculator.mutex1.Lock()

	calculator.data1[userData.UserId] = 1
	log.Infof("Bot user %s with %d reviews stored.", userData.UserId, userData.Reviews)

	calculator.mutex1.Unlock()
}

func (calculator *Calculator) AddUser(rawData string) {
	var userData rabbitmq.UserData
	json.Unmarshal([]byte(rawData), &userData)

	calculator.mutex2.Lock()

	calculator.data2[userData.UserId] = userData.Reviews
	log.Infof("User %s total reviews stored at %d.", userData.UserId, userData.Reviews)

	calculator.mutex2.Unlock()
}

func (calculator *Calculator) RetrieveMatches() []rabbitmq.UserData {
	var list []rabbitmq.UserData

	calculator.mutex1.Lock()
	for userId, _ := range calculator.data1 {
		calculator.mutex1.Unlock()

		calculator.mutex2.Lock()
		if reviews, found := calculator.data2[userId]; found {
			calculator.mutex2.Unlock()

			log.Infof("User %s has posted %d reviews, all with the same text.", userId, reviews)
			joinedData := rabbitmq.UserData {
				UserId:		userId,
				Reviews:	reviews,
			}
			list = append(list, joinedData)

			calculator.mutex1.Lock()
			delete(calculator.data1, userId);
			calculator.mutex1.Unlock()

			calculator.mutex2.Lock()
			delete(calculator.data2, userId);
			calculator.mutex2.Unlock()
			
		} else {
			calculator.mutex2.Unlock()
			log.Tracef("Still no user join match for user %s.", userId)
		}

		calculator.mutex1.Lock()
	}

	calculator.mutex1.Unlock()
	return list
}
