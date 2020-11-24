package common

import (
	"fmt"
	"sync"
	"encoding/json"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/prettiers/bot-users/rabbitmq"
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

func (builder *Builder) Save(rawData string) {
	var userData rabbitmq.UserData
	json.Unmarshal([]byte(rawData), &userData)

	builder.mutex.Lock()

	builder.data[userData.UserId] = userData.Reviews
	log.Infof("Saved user %s with %d reviews (all with the same text).", userData.UserId, userData.Reviews)

	builder.mutex.Unlock()
}

func (builder *Builder) BuildData() string {
	response := fmt.Sprintf("Users with +%d reviews (all with same text): ", builder.reviews)

	for userId, reviews := range builder.data {
		response += fmt.Sprintf("%s (%d) ; ", userId, reviews)
    }

    if len(builder.data) == 0 {
    	return response + "no users accomplish that requirements."
    } else {
    	return response[0:len(response)-3]
    }
}
