package processing

import (
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
)

func ProcessAggregations(inputs [][]byte, storingChannel chan []byte, wg *sync.WaitGroup) {
	for _, aggregatedData := range inputs {
		wg.Add(1)
		storingChannel <- aggregatedData
	}
}

func InitializeAggregationWorkers(workersPool int, storingChannel chan []byte, callback func(int, []byte), wg *sync.WaitGroup) {
	bulkNumber := 0
	bulkNumberMutex := &sync.Mutex{}

	log.Infof("Initializing %d aggregation workers.", workersPool)
	for worker := 1 ; worker <= workersPool ; worker++ {
		log.Infof("Initializing aggregation worker #%d.", worker)
		
		go func() {
			for bulk := range storingChannel {
				bulkNumberMutex.Lock()
				bulkNumber++
				innerBulk := bulkNumber
				bulkNumberMutex.Unlock()

				logb.Instance().Infof(fmt.Sprintf("Aggregated bulk #%d generated.", innerBulk), innerBulk)

				callback(innerBulk, bulk)
    			wg.Done()
			}
		}()
	}
}
