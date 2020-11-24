package common

import (
	"fmt"
	"sort"
	"sync"
	"encoding/json"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/prettiers/funniest-cities/rabbitmq"
)

type Builder struct {
	data 			[]rabbitmq.FunnyCityData
	mutex 			*sync.Mutex
}

func NewBuilder() *Builder {
	builder := &Builder {
		data:		[]rabbitmq.FunnyCityData{},
		mutex:		&sync.Mutex{},
	}

	return builder
}

func (builder *Builder) Save(rawData string) {
	var funnyCity rabbitmq.FunnyCityData
	json.Unmarshal([]byte(rawData), &funnyCity)

	builder.mutex.Lock()
	builder.data = append(builder.data, funnyCity)
	builder.mutex.Unlock()

	log.Infof("City %s saved with funniness at %d.", funnyCity.City, funnyCity.Funny)
}

func (builder *Builder) BuildTopTen() string {
	sort.SliceStable(builder.data, func(cityIdx1, cityIdx2 int) bool {
	    return builder.data[cityIdx1].Funny > builder.data[cityIdx2].Funny
	})

	var topTenCities []rabbitmq.FunnyCityData
	funnyCities := len(builder.data)
	if (funnyCities > 10) {
		log.Infof("%d cities where discarded due to not being funny enoguh.", funnyCities - 10)
		topTenCities = builder.data[0:9]
	} else {
		log.Infof("They where just %d cities with a funniness higher than 0!", funnyCities)
		topTenCities = builder.data[0:funnyCities]
	}

	response := "Top Funniest Cities: "
	for _, funnyCity := range topTenCities {
		response += fmt.Sprintf("%s w/ %dp ; ", funnyCity.City, funnyCity.Funny)
    }

    if len(topTenCities) == 0 {
    	return "no cities have funny points."
    } else {
    	return response[0:len(response)-3]
    }
}
