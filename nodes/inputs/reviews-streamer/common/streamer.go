package common

import (
	"fmt"
	"sync"
	"github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/inputs/reviews-streamer/utils"
	"github.com/LaCumbancha/reviews-analysis/nodes/inputs/reviews-streamer/rabbitmq"
)

type StreamerConfig struct {
	Instance			string
	RabbitIp			string
	RabbitPort			string
	StreamerClients		int
	WorkersPool 		int
	FunbizMappers		int
	WeekdaysMappers		int
	HashesMappers		int
	UsersMappers		int
	StarsMappers		int
}

type Streamer struct {
	connection 			*amqp.Connection
	channel 			*amqp.Channel
	innerChannel		chan string
	poolSize			int
	inputQueue 			*rabbitmq.RabbitInputQueue
	outputDirect 		*rabbitmq.RabbitOutputDirect
	endSignals 			int
}

func NewStreamer(config StreamerConfig) *Streamer {
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

	outputDirect := rabbitmq.NewRabbitOutputDirect(
		rabbitmq.OUTPUT_EXCHANGE_NAME,
		config.Instance,
		config.FunbizMappers, 
		config.WeekdaysMappers, 
		config.HashesMappers, 
		config.UsersMappers, 
		config.StarsMappers, 
		ch,
	)

	inputQueue := rabbitmq.NewRabbitInputQueue(rabbitmq.INPUT_QUEUE_NAME, ch)
	
	streamer := &Streamer {
		connection:			conn,
		channel:			ch,
		innerChannel:		make(chan string),
		poolSize:			config.WorkersPool,
		inputQueue:			inputQueue,
		outputDirect:		outputDirect,
		endSignals:			config.StreamerClients,
	}

	return streamer
}

func (streamer *Streamer) Run() {
	log.Infof("Starting to listen for streamed reviews.")

	var wg sync.WaitGroup
	wg.Add(1)
	go streamer.retrieveReviews(&wg)

	for worker := 1 ; worker <= streamer.poolSize ; worker++ {
		go func() {
			log.Infof("Initializing worker %d.", worker)
			for review := range streamer.innerChannel {
				reviewId := utils.GetReviewId(review)
				log.Infof("Processing review %s.", reviewId)
    			streamer.outputDirect.PublishReview(review, reviewId)
    			wg.Done()
			}
		}()
	}
	
    // Using WaitGroups to avoid closing the RabbitMQ connection before all messages are sent.
    wg.Wait()

    // Publishing end messages.
    streamer.outputDirect.PublishFinish()
}

func (streamer *Streamer) retrieveReviews(wg *sync.WaitGroup) {
	var endSignalsMutex = &sync.Mutex{}
	var endSignals = make(map[string]int)

	for message := range streamer.inputQueue.ConsumeReviews() {
		messageBody := string(message.Body)

		if rabbitmq.IsEndMessage(messageBody) {
			streamer.processEndSignal(messageBody, endSignals, endSignalsMutex, wg)
		} else {
			wg.Add(1)
			reviewId := utils.GetReviewId(messageBody)
			log.Infof("Review '%s' received.", reviewId)
			streamer.innerChannel <- messageBody
		}
	}

	wg.Done()
}

func (streamer *Streamer) processEndSignal(newMessage string, endSignals map[string]int, mutex *sync.Mutex, wg *sync.WaitGroup) {
	mutex.Lock()
	endSignals[newMessage] = endSignals[newMessage] + 1
	newSignal := endSignals[newMessage] == 1
	signalsReceived := len(endSignals)
	mutex.Unlock()

	log.Infof("End-Message #%d received.", signalsReceived)

	// Waiting for the total needed End-Signals to send the own End-Message.
	if (signalsReceived == streamer.endSignals) && newSignal {
		log.Infof("All End-Messages were received.")
		wg.Done()
	}
}


func (streamer *Streamer) Stop() {
	log.Infof("Closing Reviews-Streamer connections.")
	streamer.connection.Close()
	streamer.channel.Close()
}
