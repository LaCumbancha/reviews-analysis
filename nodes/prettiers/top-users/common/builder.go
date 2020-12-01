package common

import (
	"fmt"
	"sync"
	"encoding/json"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/prettiers/top-users/rabbitmq"
)

type Builder struct {
	data 			map[string]int
	mutex 			*sync.Mutex
	reviews 		int
}

func NewBuilder(minReviews int) *Builder {
	builder := &Builder {
		data:		make(map[string]int),
		mutex:		&sync.Mutex{},
		reviews:	minReviews,
	}

	return builder
}

func (builder *Builder) Save(rawUserDataBulk string) {
	var userDataList []rabbitmq.UserData
	json.Unmarshal([]byte(rawUserDataBulk), &userDataList)

	for _, userData := range userDataList {
		builder.mutex.Lock()

		if oldReviews, found := builder.data[userData.UserId]; found {
		    log.Warnf("User %s was already stored with %d reviews (new value: %d).", userData.UserId, oldReviews, userData.Reviews)
		} else {
			builder.data[userData.UserId] = userData.Reviews
		}
		
		builder.mutex.Unlock()
	}
}

func (builder *Builder) BuildData() string {
	response := fmt.Sprintf("Users with +%d reviews: ", builder.reviews)

	totalReviews := 0
	for _, reviews := range builder.data {
		totalReviews += reviews
    }

    totalUsers := len(builder.data)
    if totalUsers == 0 {
    	return response + "no users accomplish that requirements."
    } else {
    	return response + fmt.Sprintf("%d, with %d total reviews (average: %3.2f per user).", totalUsers, totalReviews, float32(totalReviews)/float32(totalUsers))
    }
}
