package main

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/aggregators/weekday/common"
	"github.com/LaCumbancha/reviews-analysis/nodes/aggregators/weekday/utils"
)

func InitConfig() (*viper.Viper, *viper.Viper, error) {
	configEnv := viper.New()

	// Configure viper to read env variables with the WEEKDAYAGG prefix
	configEnv.AutomaticEnv()
	configEnv.SetEnvPrefix("weekdayagg")

	// Add env variables supported
	configEnv.BindEnv("rabbitmq", "ip")
	configEnv.BindEnv("rabbitmq", "port")
	configEnv.BindEnv("input", "topic")
	configEnv.BindEnv("weekday", "mappers")

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

func failOnError(err error, msg string) {
  if err != nil {
    log.Fatalf("%s: %s", msg, err)
  }
}

func main() {
	log.SetLevel(log.TraceLevel)
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

	inputTopic := utils.GetConfigString(configEnv, configFile, "input_topic")
	
	if inputTopic == "" {
		log.Fatalf("InputTopic variable missing")
	}

	weekdayMappers := utils.GetConfigInt(configEnv, configFile, "weekday_mappers")
	
	if weekdayMappers == 0 {
		log.Fatalf("WeekdayMappers variable missing")
	}

	aggregatorConfig := common.AggregatorConfig {
		RabbitIp:			rabbitIp,
		RabbitPort:			rabbitPort,
		InputTopic: 		inputTopic,
		WeekdayMappers:		weekdayMappers,
	}

	aggregator := common.NewAggregator(aggregatorConfig)
	aggregator.Run()
	aggregator.Stop()
}
