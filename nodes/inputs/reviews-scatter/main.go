package main

import (
	"fmt"
	"time"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/inputs/reviews-scatter/logger"
	"github.com/LaCumbancha/reviews-analysis/nodes/inputs/reviews-scatter/common"
	"github.com/LaCumbancha/reviews-analysis/nodes/inputs/reviews-scatter/utils"
)

func InitConfig() (*viper.Viper, *viper.Viper, error) {
	configEnv := viper.New()

	// Configure viper to read env variables with the RVWSCA_ prefix
	configEnv.AutomaticEnv()
	configEnv.SetEnvPrefix("rvwsca")

	// Add env variables supported
	configEnv.BindEnv("instance")
	configEnv.BindEnv("reviews", "data")
	configEnv.BindEnv("rabbitmq", "ip")
	configEnv.BindEnv("rabbitmq", "port")
	configEnv.BindEnv("bulk", "size")
	configEnv.BindEnv("workers", "pool")
	configEnv.BindEnv("funbiz", "mappers")
	configEnv.BindEnv("weekdays", "mappers")
	configEnv.BindEnv("hashes", "mappers")
	configEnv.BindEnv("users", "mappers")
	configEnv.BindEnv("stars", "mappers")
	configEnv.BindEnv("log", "bulk", "size")
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

	instance := utils.GetConfigString(configEnv, configFile, "instance")
	
	if instance == "" {
		log.Fatalf("Instance variable missing")
	}

	reviewsData := utils.GetConfigString(configEnv, configFile, "reviews_data")
	
	if reviewsData == "" {
		log.Fatalf("ReviewsData variable missing")
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

	funbizMappers := utils.GetConfigInt(configEnv, configFile, "funbiz_mappers")
	
	if funbizMappers == 0 {
		log.Fatalf("FunbizMappers variable missing")
	}

	weekdaysMappers := utils.GetConfigInt(configEnv, configFile, "weekdays_mappers")
	
	if weekdaysMappers == 0 {
		log.Fatalf("WeekdaysMappers variable missing")
	}

	hashesMappers := utils.GetConfigInt(configEnv, configFile, "hashes_mappers")
	
	if hashesMappers == 0 {
		log.Fatalf("HashesMappers variable missing")
	}

	usersMappers := utils.GetConfigInt(configEnv, configFile, "users_mappers")
	
	if usersMappers == 0 {
		log.Fatalf("UsersMappers variable missing")
	}

	starsMappers := utils.GetConfigInt(configEnv, configFile, "stars_mappers")
	
	if starsMappers == 0 {
		log.Fatalf("StarsMappers variable missing")
	}

	scatterConfig := common.ScatterConfig {
		Instance:				instance,
		Data:					reviewsData,
		RabbitIp:				rabbitIp,
		RabbitPort:				rabbitPort,
		BulkSize:				bulkSize,
		WorkersPool:			workersPool,
		FunbizMappers:			funbizMappers,
		WeekdaysMappers:		weekdaysMappers,
		HashesMappers:			hashesMappers,
		UsersMappers:			usersMappers,
		StarsMappers:			starsMappers,
	}

	// Initializing custom logger.
	logBulkRate := utils.GetConfigInt(configEnv, configFile, "log_bulk_rate")
	
	if logBulkRate == 0 {
		log.Fatalf("LogBulkRate variable missing")
	}

	logger.Instance().SetBulkRate(logBulkRate)

	// Waiting for all other nodes to correctly configure before starting sending reviews.
	scatter := common.NewScatter(scatterConfig)
	time.Sleep(2000 * time.Millisecond)

	scatter.Run()
	scatter.Stop()
}
