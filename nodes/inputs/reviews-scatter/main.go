package main

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/inputs/reviews-scatter/common"
	"github.com/LaCumbancha/reviews-analysis/nodes/inputs/reviews-scatter/utils"
)

func InitConfig() (*viper.Viper, *viper.Viper, error) {
	configEnv := viper.New()

	// Configure viper to read env variables with the REVSCA_ prefix
	configEnv.AutomaticEnv()
	configEnv.SetEnvPrefix("rvwsca")

	// Add env variables supported
	configEnv.BindEnv("reviews", "data")
	configEnv.BindEnv("rabbitmq", "ip")
	configEnv.BindEnv("rabbitmq", "port")
	configEnv.BindEnv("scatter", "queue", "name")
	configEnv.BindEnv("config", "file")

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

	reviewsData := utils.GetConfigValue(configEnv, configFile, "reviews_data")
	
	if reviewsData == "" {
		log.Fatalf("ReviewsData variable missing")
	}

	rabbitIp := utils.GetConfigValue(configEnv, configFile, "rabbitmq_ip")
	
	if rabbitIp == "" {
		log.Fatalf("RabbitIp variable missing")
	}

	rabbitPort := utils.GetConfigValue(configEnv, configFile, "rabbitmq_port")
	
	if rabbitPort == "" {
		log.Fatalf("RabbitPort variable missing")
	}

	scatterQueueName := utils.GetConfigValue(configEnv, configFile, "scatter_queue_name")
	
	if scatterQueueName == "" {
		log.Fatalf("ScatterQueueName variable missing")
	}

	reviewsScatterConfig := common.ReviewsScatterConfig {
		Data:					reviewsData,
		RabbitIp:				rabbitIp,
		RabbitPort:				rabbitPort,
		ScatterQueueName:		scatterQueueName,
	}

	reviewsScatter := common.NewReviewsScatter(reviewsScatterConfig)
	reviewsScatter.Run()
	reviewsScatter.Stop()
}
