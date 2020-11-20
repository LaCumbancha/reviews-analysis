package main

import (
    "fmt"
    "strings"
	"github.com/spf13/viper"
    "github.com/streadway/amqp"
	log "github.com/sirupsen/logrus"

    "github.com/LaCumbancha/reviews-analysis/nodes/outputs/mock-receiver/receivers"
)

func InitConfig() (*viper.Viper, error) {
    configEnv := viper.New()

    // Configure viper to read env variables with the MOCK_ prefix
    configEnv.AutomaticEnv()
    configEnv.SetEnvPrefix("mock")

    // Add env variables supported
    configEnv.BindEnv("rabbitmq", "ip")
    configEnv.BindEnv("rabbitmq", "port")
    configEnv.BindEnv("type")
    configEnv.BindEnv("exchange")
    configEnv.BindEnv("queue")
    configEnv.BindEnv("topic")

    return configEnv, nil
}

func main() {
	log.SetLevel(log.DebugLevel)
	configEnv, err := InitConfig()

    if err != nil {
        log.Fatalf("Fatal error loading configuration. Err: '%s'", err)
    }

    rabbitmqIp := configEnv.GetString("rabbitmq_ip")
    if rabbitmqIp == "" {
        log.Fatalf("RabbitmqIp variable missing")
    }

    rabbitmqPort := configEnv.GetString("rabbitmq_port")
    if rabbitmqPort == "" {
        log.Fatalf("RabbitmqPort variable missing")
    }

    rtype := configEnv.GetString("type")
    if rtype == "" {
        log.Fatalf("Type variable missing")
    }

    connection, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%s/", rabbitmqIp, rabbitmqPort))
    if err != nil {
        log.Fatalf("Failed to connect to RabbitMQ. Err: %s", err)
    }

	channel, err := connection.Channel()
    if err != nil {
        log.Fatalf("Failed to open a RabbitMQ channel. Err: %s", err)
    }

	switch strings.ToUpper(rtype) {
	case "DIRECT":
		receivers.Direct(configEnv, connection, channel)
	case "QUEUE":
		receivers.Queue(configEnv, connection, channel)
	default:
		log.Fatalf("Invalid MOCK_TYPE.")
	}
}
