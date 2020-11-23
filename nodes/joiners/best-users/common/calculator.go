package common

import (
	"sync"
	"encoding/json"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/joiners/best-users/rabbitmq"
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

func (calculator *Calculator) AddBestUser(rawData string) {
	var userData rabbitmq.UserData
	json.Unmarshal([]byte(rawData), &userData)

	calculator.mutex1.Lock()

	calculator.data1[userData.UserId] = userData.Reviews
	log.Infof("User %s 5-stars reviews stored at %d.", userData.UserId, userData.Reviews)

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

func (calculator *Calculator) RetrieveData() []rabbitmq.UserData {
	var list []rabbitmq.UserData

	calculator.mutex1.Lock()
	for userId, bestReviews := range calculator.data1 {
		calculator.mutex1.Unlock()

		calculator.mutex2.Lock()
		if totalReviews, found := calculator.data2[userId]; found {
			calculator.mutex2.Unlock()

			if bestReviews == totalReviews {
				log.Infof("All user %s reviews where rated with 5 stars.", userId)
				joinedData := rabbitmq.UserData {
					UserId:		userId,
					Reviews:	totalReviews,
				}
				list = append(list, joinedData)
			} else {
				log.Debugf("User %s had %d reviews but only %d where rated with 5 stars.", userId, totalReviews, bestReviews)
			}


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
