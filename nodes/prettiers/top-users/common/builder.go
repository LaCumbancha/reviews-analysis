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

	for userId, reviews := range builder.data {
		response += fmt.Sprintf("%s (%d) ; ", userId, reviews)
    }

    if len(builder.data) == 0 {
    	return response + "no users accomplish that requirements."
    } else {
    	return response[0:len(response)-3]
    }
}
