package common

import (
	"fmt"
	"sync"
	"encoding/json"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/mappers/histogram/rabbitmq"
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
	var weekdayData rabbitmq.WeekdayData
	json.Unmarshal([]byte(rawData), &weekdayData)

	builder.mutex.Lock()

	builder.data[weekdayData.Weekday] = weekdayData.Reviews
	log.Infof("Saved weekday %s reviews at %d.", weekdayData.Weekday, weekdayData.Reviews)

	builder.mutex.Unlock()
}

func (builder *Builder) BuildData() string {
	return fmt.Sprintf(
		"Sunday: %d ; Monday: %d ; Tuesday: %d ; Wednesday: %d ; Thurdsay : %d ; Friday: %d ; Saturday: %d",
		builder.data["Sunday"],
		builder.data["Monday"],
		builder.data["Tuesday"],
		builder.data["Wednesday"],
		builder.data["Thursday"],
		builder.data["Friday"],
		builder.data["Saturday"],
	)
}
