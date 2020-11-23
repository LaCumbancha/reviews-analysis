package main

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/mappers/stars/common"
	"github.com/LaCumbancha/reviews-analysis/nodes/mappers/stars/utils"
)

func InitConfig() (*viper.Viper, *viper.Viper, error) {
	configEnv := viper.New()

	// Configure viper to read env variables with the STARSMAP prefix
	configEnv.AutomaticEnv()
	configEnv.SetEnvPrefix("starsmap")

	// Add env variables supported
	configEnv.BindEnv("instance")
	configEnv.BindEnv("rabbitmq", "ip")
	configEnv.BindEnv("rabbitmq", "port")
	configEnv.BindEnv("reviews", "inputs")
	configEnv.BindEnv("stars", "filters")
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

	starsFilters := utils.GetConfigInt(configEnv, configFile, "stars_filters")
	
	if starsFilters == 0 {
		log.Fatalf("StarsFilters variable missing")
	}

	mapperConfig := common.MapperConfig {
		Instance:			instance,
		RabbitIp:			rabbitIp,
		RabbitPort:			rabbitPort,
		ReviewsInputs:		reviewsInputs,
		StarsFilters:		starsFilters,
	}

	mapper := common.NewMapper(mapperConfig)
	mapper.Run()
	mapper.Stop()
}
