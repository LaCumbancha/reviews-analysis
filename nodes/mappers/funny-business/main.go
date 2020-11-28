package main

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/LaCumbancha/reviews-analysis/nodes/mappers/funny-business/common"
	"github.com/LaCumbancha/reviews-analysis/nodes/mappers/funny-business/utils"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/nodes/mappers/funny-business/logger"
)

func InitConfig() (*viper.Viper, *viper.Viper, error) {
	configEnv := viper.New()

	// Configure viper to read env variables with the FUNBIZMAP prefix
	configEnv.AutomaticEnv()
	configEnv.SetEnvPrefix("funbizmap")

	// Add env variables supported
	configEnv.BindEnv("instance")
	configEnv.BindEnv("rabbitmq", "ip")
	configEnv.BindEnv("rabbitmq", "port")
	configEnv.BindEnv("reviews", "inputs")
	configEnv.BindEnv("funbiz", "filters")
	configEnv.BindEnv("reviews", "flows")
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

	funbizFilters := utils.GetConfigInt(configEnv, configFile, "funbiz_filters")
	
	if funbizFilters == 0 {
		log.Fatalf("FunbizFilters variable missing")
	}

	mapperConfig := common.MapperConfig {
		Instance:			instance,
		RabbitIp:			rabbitIp,
		RabbitPort:			rabbitPort,
		ReviewsInputs:		reviewsInputs,
		FunbizFilters:		funbizFilters,
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
