package main

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/outputs/sink/common"
	"github.com/LaCumbancha/reviews-analysis/nodes/outputs/sink/utils"
)

func InitConfig() (*viper.Viper, *viper.Viper, error) {
	configEnv := viper.New()

	// Configure viper to read env variables with the SINK_ prefix
	configEnv.AutomaticEnv()
	configEnv.SetEnvPrefix("sink")

	// Add env variables supported
	configEnv.BindEnv("rabbitmq", "ip")
	configEnv.BindEnv("rabbitmq", "port")
	configEnv.BindEnv("funny", "city", "queue", "name")
	configEnv.BindEnv("weekday", "histogram", "queue", "name")
	configEnv.BindEnv("top", "users", "queue", "name")
	configEnv.BindEnv("best", "users", "queue", "name")
	configEnv.BindEnv("bot", "users", "queue", "name")

	// Read config file if it's present
	var configFile = viper.New()
	if configFileName := configEnv.GetString("config_file"); configFileName != "" {
		path, file, ctype := utils.GetConfigFile(configFileName)

		configFile.SetConfigName(file)
		configFile.SetConfigType(ctype)
		configFile.AddConfigPath(path)
		err := configFile.ReadInConfig()

		if err != nil {
			return nil, nil, errors.Wrapf(err, fmt.Sprintf("Couldn't load config file"))
		}
	}

	return configEnv, configFile, nil
}

func main() {
	log.SetLevel(log.DebugLevel)
	configEnv, configFile, err := InitConfig()

	if err != nil {
		log.Fatalf("Fatal error loading configuration. Err: '%s'", err)
	}

	rabbitIp := utils.GetConfigString(configEnv, configFile, "rabbitmq_ip")
	
	if rabbitIp == "" {
		log.Fatalf("RabbitIp variable missing")
	}

	rabbitPort := utils.GetConfigString(configEnv, configFile, "rabbitmq_port")
	
	if rabbitPort == "" {
		log.Fatalf("RabbitPort variable missing")
	}

	funnyCityQueueName := utils.GetConfigString(configEnv, configFile, "funny_city_queue_name")
	
	if funnyCityQueueName == "" {
		log.Fatalf("FunnyCityQueueName variable missing")
	}

	weekdayHistogramQueueName := utils.GetConfigString(configEnv, configFile, "weekday_histogram_queue_name")
	
	if weekdayHistogramQueueName == "" {
		log.Fatalf("WeekdayHistogramQueueName variable missing")
	}

	topUsersQueueName := utils.GetConfigString(configEnv, configFile, "top_users_queue_name")
	
	if topUsersQueueName == "" {
		log.Fatalf("TopUsersQueueName variable missing")
	}

	bestUsersQueueName := utils.GetConfigString(configEnv, configFile, "best_users_queue_name")
	
	if bestUsersQueueName == "" {
		log.Fatalf("BestUsersQueueName variable missing")
	}

	botUsersQueueName := utils.GetConfigString(configEnv, configFile, "bot_users_queue_name")
	
	if botUsersQueueName == "" {
		log.Fatalf("BotUsersQueueName variable missing")
	}

	sinkConfig := common.SinkConfig {
		RabbitIp:						rabbitIp,
		RabbitPort:						rabbitPort,
		FunnyCityQueueName:				funnyCityQueueName,
		WeekdayHistogramQueueName:		weekdayHistogramQueueName,
		TopUsersQueueName:				topUsersQueueName,
		BestUsersQueueName:				bestUsersQueueName,
		BotUsersQueueName:				botUsersQueueName,
	}

	sink := common.NewSink(sinkConfig)
	sink.Run()
	sink.Stop()
}
