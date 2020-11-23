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
}

func NewBuilder() *Builder {
	builder := &Builder {
		data:		make(map[string]int),
		mutex:		&sync.Mutex{},
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
	response := ""

	for userId, reviews := range builder.data {
		response += fmt.Sprintf("%s (%d) ; ", userId, reviews)
    }

    responseLength := len(response)
    if responseLength == 0 {
    	return "No users have +5 reviews (all with the same text)."
    } else {
    	return response[0:responseLength-3]
    }
}
