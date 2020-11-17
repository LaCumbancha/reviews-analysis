package main

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/mappers/funbiz-mapper/common"
	"github.com/LaCumbancha/reviews-analysis/nodes/mappers/funbiz-mapper/utils"
)

func InitConfig() (*viper.Viper, *viper.Viper, error) {
	configEnv := viper.New()

	// Configure viper to read env variables with the REVSCA_ prefix
	configEnv.AutomaticEnv()
	configEnv.SetEnvPrefix("funbiz")

	// Add env variables supported
	configEnv.BindEnv("rabbitmq", "ip")
	configEnv.BindEnv("rabbitmq", "port")
	configEnv.BindEnv("input", "queue", "name")
	configEnv.BindEnv("output", "queue", "name")
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

func failOnError(err error, msg string) {
  if err != nil {
    log.Fatalf("%s: %s", msg, err)
  }
}

func main() {
	log.SetLevel(log.DebugLevel)
	configEnv, configFile, err := InitConfig()

	if err != nil {
		log.Fatalf("Fatal error loading configuration. Err: '%s'", err)
	}

	rabbitIp := utils.GetConfigValue(configEnv, configFile, "rabbitmq_ip")
	
	if rabbitIp == "" {
		log.Fatalf("RabbitIp variable missing")
	}

	rabbitPort := utils.GetConfigValue(configEnv, configFile, "rabbitmq_port")
	
	if rabbitPort == "" {
		log.Fatalf("RabbitPort variable missing")
	}

	inputQueueName := utils.GetConfigValue(configEnv, configFile, "input_queue_name")
	
	if inputQueueName == "" {
		log.Fatalf("InputQueueName variable missing")
	}

	outputQueueName := utils.GetConfigValue(configEnv, configFile, "output_queue_name")
	
	if outputQueueName == "" {
		log.Fatalf("OutputQueueName variable missing")
	}

	mapperConfig := common.MapperConfig {
		RabbitIp:			rabbitIp,
		RabbitPort:			rabbitPort,
		InputQueueName:		inputQueueName,
		OutputQueueName:	outputQueueName,
	}

	mapper := common.NewMapper(mapperConfig)
	mapper.Run()
	mapper.Stop()
}
