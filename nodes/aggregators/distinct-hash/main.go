package main

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/aggregators/distinct-hash/common"
	"github.com/LaCumbancha/reviews-analysis/nodes/aggregators/distinct-hash/utils"
)

func InitConfig() (*viper.Viper, *viper.Viper, error) {
	configEnv := viper.New()

	// Configure viper to read env variables with the DISHASHAGG prefix
	configEnv.AutomaticEnv()
	configEnv.SetEnvPrefix("dishashagg")

	// Add env variables supported
	configEnv.BindEnv("instance")
	configEnv.BindEnv("rabbitmq", "ip")
	configEnv.BindEnv("rabbitmq", "port")
	configEnv.BindEnv("input", "topic")
	configEnv.BindEnv("hash", "aggregators")
	configEnv.BindEnv("dishash", "filters")
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
	log.SetLevel(log.TraceLevel)
	configEnv, configFile, err := InitConfig()

	if err != nil {
		log.Fatalf("Fatal error loading configuration. Err: '%s'", err)
	}

	instance := utils.GetConfigString(configEnv, configFile, "instance")
	
	if instance == "" {
		log.Fatalf("Instance variable missing")
	}

	rabbitIp := utils.GetConfigString(configEnv, configFile, "rabbitmq_ip")
	
	if rabbitIp == "" {
		log.Fatalf("RabbitIp variable missing")
	}

	rabbitPort := utils.GetConfigString(configEnv, configFile, "rabbitmq_port")
	
	if rabbitPort == "" {
		log.Fatalf("RabbitPort variable missing")
	}

	inputTopic := utils.GetConfigString(configEnv, configFile, "input_topic")
	
	if inputTopic == "" {
		log.Fatalf("InputTopic variable missing")
	}

	hashAggregators := utils.GetConfigInt(configEnv, configFile, "hash_aggregators")
	
	if hashAggregators == 0 {
		log.Fatalf("HashAggregators variable missing")
	}

	dishashFilters := utils.GetConfigInt(configEnv, configFile, "dishash_filters")
	
	if dishashFilters == 0 {
		log.Fatalf("DishashFilters variable missing")
	}

	aggregatorConfig := common.AggregatorConfig {
		Instance:			instance,
		RabbitIp:			rabbitIp,
		RabbitPort:			rabbitPort,
		InputTopic: 		inputTopic,
		HashAggregators:	hashAggregators,
		DishashFilters:		dishashFilters,
	}

	aggregator := common.NewAggregator(aggregatorConfig)
	aggregator.Run()
	aggregator.Stop()
}
