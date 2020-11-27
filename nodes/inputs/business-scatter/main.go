package main

import (
	"fmt"
	"time"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/inputs/business-scatter/utils"
	"github.com/LaCumbancha/reviews-analysis/nodes/inputs/business-scatter/common"
)

func InitConfig() (*viper.Viper, *viper.Viper, error) {
	configEnv := viper.New()

	// Configure viper to read env variables with the RVWSCA_ prefix
	configEnv.AutomaticEnv()
	configEnv.SetEnvPrefix("bizsca")

	// Add env variables supported
	configEnv.BindEnv("business", "data")
	configEnv.BindEnv("rabbitmq", "ip")
	configEnv.BindEnv("rabbitmq", "port")
	configEnv.BindEnv("bulk", "size")
	configEnv.BindEnv("workers", "pool")
	configEnv.BindEnv("citbiz", "mappers")
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

	businessData := utils.GetConfigString(configEnv, configFile, "business_data")
	
	if businessData == "" {
		log.Fatalf("BusinessData variable missing")
	}

	rabbitIp := utils.GetConfigString(configEnv, configFile, "rabbitmq_ip")
	
	if rabbitIp == "" {
		log.Fatalf("RabbitIp variable missing")
	}

	rabbitPort := utils.GetConfigString(configEnv, configFile, "rabbitmq_port")
	
	if rabbitPort == "" {
		log.Fatalf("RabbitPort variable missing")
	}

	bulkSize := utils.GetConfigInt(configEnv, configFile, "bulk_size")
	
	if bulkSize == 0 {
		log.Fatalf("BulkSize variable missing")
	}

	workersPool := utils.GetConfigInt(configEnv, configFile, "workers_pool")
	
	if workersPool == 0 {
		log.Fatalf("WorkersPool variable missing")
	}

	citbizMappers := utils.GetConfigInt(configEnv, configFile, "citbiz_mappers")
	
	if citbizMappers == 0 {
		log.Fatalf("CitbizMappers variable missing")
	}

	scatterConfig := common.ScatterConfig {
		Data:					businessData,
		RabbitIp:				rabbitIp,
		RabbitPort:				rabbitPort,
		BulkSize:				bulkSize,
		WorkersPool:			workersPool,
		CitbizMappers:			citbizMappers,
	}

	scatter := common.NewScatter(scatterConfig)

	// Waiting for all other nodes to correctly configure before starting sending reviews.
	time.Sleep(2000 * time.Millisecond)
	scatter.Run()
	scatter.Stop()
}
