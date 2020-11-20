package main

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/filters/funny-business/common"
	"github.com/LaCumbancha/reviews-analysis/nodes/filters/funny-business/utils"
)

func InitConfig() (*viper.Viper, *viper.Viper, error) {
	configEnv := viper.New()

	// Configure viper to read env variables with the FUNBIZFIL prefix
	configEnv.AutomaticEnv()
	configEnv.SetEnvPrefix("funbizfil")

	// Add env variables supported
	configEnv.BindEnv("rabbitmq", "ip")
	configEnv.BindEnv("rabbitmq", "port")
	configEnv.BindEnv("funbiz", "mappers")
	configEnv.BindEnv("funbiz", "aggregators")

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

	funbizMappers := utils.GetConfigInt(configEnv, configFile, "funbiz_mappers")
	
	if funbizMappers == 0 {
		log.Fatalf("FunbizMappers variable missing")
	}

	funbizAggregators := utils.GetConfigInt(configEnv, configFile, "funbiz_aggregators")
	
	if funbizAggregators == 0 {
		log.Fatalf("FunbizAggregators variable missing")
	}

	filterConfig := common.FilterConfig {
		RabbitIp:			rabbitIp,
		RabbitPort:			rabbitPort,
		FunbizMappers:		funbizMappers,
		FunbizAggregators:	funbizAggregators,
	}

	filter := common.NewFilter(filterConfig)
	filter.Run()
	filter.Stop()
}
