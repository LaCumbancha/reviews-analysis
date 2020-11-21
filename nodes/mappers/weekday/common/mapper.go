package common

import (
	"fmt"
	"time"
	"sync"
	"strconv"
	"encoding/json"
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/mappers/weekday/utils"
	"github.com/LaCumbancha/reviews-analysis/nodes/mappers/weekday/rabbitmq"
)

type MapperConfig struct {
	RabbitIp			string
	RabbitPort			string
	WeekdayAggregators 	int
}

type Mapper struct {
	connection 		*amqp.Connection
	channel 		*amqp.Channel
	inputDirect 	*rabbitmq.RabbitInputDirect
	outputDirect 	*rabbitmq.RabbitOutputDirect
}

func NewMapper(config MapperConfig) *Mapper {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%s/", config.RabbitIp, config.RabbitPort))
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ at (%s, %s). Err: '%s'", config.RabbitIp, config.RabbitPort, err)
	} else {
		log.Infof("Connected to RabbitMQ at (%s, %s).", config.RabbitIp, config.RabbitPort)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a RabbitMQ channel. Err: '%s'", err)
	} else {
		log.Infof("RabbitMQ channel opened.")
	}

	inputDirect := rabbitmq.NewRabbitInputDirect(rabbitmq.INPUT_EXCHANGE_NAME, ch)
	outputDirect := rabbitmq.NewRabbitOutputDirect(rabbitmq.OUTPUT_EXCHANGE_NAME, config.WeekdayAggregators, ch)
	mapper := &Mapper {
		connection:		conn,
		channel:		ch,
		inputDirect:	inputDirect,
		outputDirect:	outputDirect,
	}

	return mapper
}

func (mapper *Mapper) Run() {
	log.Infof("Starting to listen for reviews.")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for message := range mapper.inputDirect.ConsumeReviews() {
			messageBody := string(message.Body)

			if messageBody == rabbitmq.END_MESSAGE {
				log.Infof("End-Message received.")
				wg.Done()
				//rabbitmq.AckMessage(&message, rabbitmq.END_MESSAGE)
			} else {
				review := messageBody
				log.Infof("Review %s received.", utils.GetReviewId(review))

				wg.Add(1)
				go func() {
					mapper.processReview(review)
					//rabbitmq.AckMessage(&message, utils.GetReviewId(review))
					wg.Done()
				}()
			}
		}
	}()
	
    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are sent.
    wg.Wait()

    // Publishing end messages.
    mapper.outputDirect.PublishFinish()
}

func (mapper *Mapper) processReview(rawReview string) {
	var fullReview rabbitmq.FullReview
	json.Unmarshal([]byte(rawReview), &fullReview)

	reviewDate, err := time.Parse("2006-01-02 15:04:05", fullReview.Date)
	if err != nil {
		log.Errorf("Error parsing date from review %s (given date: %s)", fullReview.ReviewId, fullReview.Date)
	} else {
		reviewWeekday := reviewDate.Weekday()
		mappedReview := &rabbitmq.WeekdayData {
			Weekday:	reviewWeekday.String(),
		}

		data, err := json.Marshal(mappedReview)
		if err != nil {
			log.Errorf("Error generating Json from (%s). Err: '%s'", mappedReview, err)
		} else {
			mapper.outputDirect.PublishData(data, strconv.Itoa(int(reviewWeekday)))
		}
	}
}

func (mapper *Mapper) Stop() {
	log.Infof("Closing Weekday Mapper connections.")
	mapper.connection.Close()
	mapper.channel.Close()
}
