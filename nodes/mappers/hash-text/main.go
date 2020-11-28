package main

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/LaCumbancha/reviews-analysis/nodes/mappers/hash-text/common"
	"github.com/LaCumbancha/reviews-analysis/nodes/mappers/hash-text/utils"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/nodes/mappers/hash-text/logger"
)

func InitConfig() (*viper.Viper, *viper.Viper, error) {
	configEnv := viper.New()

	// Configure viper to read env variables with the USERMAP prefix
	configEnv.AutomaticEnv()
	configEnv.SetEnvPrefix("hashmap")

	// Add env variables supported
	configEnv.BindEnv("instance")
	configEnv.BindEnv("rabbitmq", "ip")
	configEnv.BindEnv("rabbitmq", "port")
	configEnv.BindEnv("reviews", "inputs")
	configEnv.BindEnv("hash", "aggregators")
	configEnv.BindEnv("log", "bulk", "rate")
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

	reviewsInputs := utils.GetConfigInt(configEnv, configFile, "reviews_inputs")
	
	if reviewsInputs == 0 {
		log.Fatalf("ReviewsInputs variable missing")
	}

	hashAggregators := utils.GetConfigInt(configEnv, configFile, "hash_aggregators")
	
	if hashAggregators == 0 {
		log.Fatalf("HashAggregators variable missing")
	}

	mapperConfig := common.MapperConfig {
		Instance:			instance,
		RabbitIp:			rabbitIp,
		RabbitPort:			rabbitPort,
		ReviewsInputs:		reviewsInputs,
		HashAggregators:	hashAggregators,
	}

	// Initializing custom logger.
	logBulkRate := utils.GetConfigInt(configEnv, configFile, "log_bulk_rate")
	
	if logBulkRate == 0 {
		log.Fatalf("LogBulkRate variable missing")
	}

	logb.Instance().SetBulkRate(logBulkRate)

	// Initializing mapper.
	mapper := common.NewMapper(mapperConfig)
	mapper.Run()
	mapper.Stop()
}
