package main

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/joiners/funny-city/common"
	"github.com/LaCumbancha/reviews-analysis/nodes/joiners/funny-city/utils"
)

func InitConfig() (*viper.Viper, *viper.Viper, error) {
	configEnv := viper.New()

	// Configure viper to read env variables with the FUNBIZAGG prefix
	configEnv.AutomaticEnv()
	configEnv.SetEnvPrefix("funcitjoin")

	// Add env variables supported
	configEnv.BindEnv("rabbitmq", "ip")
	configEnv.BindEnv("rabbitmq", "port")
	configEnv.BindEnv("input", "topic")
	configEnv.BindEnv("funbiz", "aggregators")
	configEnv.BindEnv("citbiz", "mappers")
	configEnv.BindEnv("funcit", "aggregators")

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

	funbizAggregators := utils.GetConfigInt(configEnv, configFile, "funbiz_aggregators")
	
	if funbizAggregators == 0 {
		log.Fatalf("FunbizAggregators variable missing")
	}

	citbizMappers := utils.GetConfigInt(configEnv, configFile, "citbiz_mappers")
	
	if citbizMappers == 0 {
		log.Fatalf("CitbizMappers variable missing")
	}

	funcitAggregators := utils.GetConfigInt(configEnv, configFile, "funcit_aggregators")
	
	if funcitAggregators == 0 {
		log.Fatalf("FuncitAggregators variable missing")
	}

	joinerConfig := common.JoinerConfig {
		RabbitIp:			rabbitIp,
		RabbitPort:			rabbitPort,
		InputTopic: 		inputTopic,
		FunbizAggregators:	funbizAggregators,
		CitbizMappers:		citbizMappers,
		FuncitAggregators:	funcitAggregators,
	}

	joiner := common.NewJoiner(joinerConfig)
	joiner.Run()
	joiner.Stop()
}
