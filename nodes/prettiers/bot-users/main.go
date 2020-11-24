package main

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/reviews-analysis/nodes/prettiers/bot-users/common"
	"github.com/LaCumbancha/reviews-analysis/nodes/prettiers/bot-users/utils"
)

func InitConfig() (*viper.Viper, *viper.Viper, error) {
	configEnv := viper.New()

	// Configure viper to read env variables with the BESTUSER prefix
	configEnv.AutomaticEnv()
	configEnv.SetEnvPrefix("botuserpre")

	// Add env variables supported
	configEnv.BindEnv("rabbitmq", "ip")
	configEnv.BindEnv("rabbitmq", "port")
	configEnv.BindEnv("min", "reviews")
	configEnv.BindEnv("botuser", "joiners")
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

	rabbitIp := utils.GetConfigString(configEnv, configFile, "rabbitmq_ip")
	
	if rabbitIp == "" {
		log.Fatalf("RabbitIp variable missing")
	}

	rabbitPort := utils.GetConfigString(configEnv, configFile, "rabbitmq_port")
	
	if rabbitPort == "" {
		log.Fatalf("RabbitPort variable missing")
	}

	minReviews := utils.GetConfigInt(configEnv, configFile, "min_reviews")
	
	if minReviews == 0 {
		log.Fatalf("MinReviews variable missing")
	}

	botUserJoiners := utils.GetConfigInt(configEnv, configFile, "botuser_joiners")
	
	if botUserJoiners == 0 {
		log.Fatalf("BotUserJoiners variable missing")
	}

	mapperConfig := common.MapperConfig {
		RabbitIp:			rabbitIp,
		RabbitPort:			rabbitPort,
		MinReviews:			minReviews,
		BotUserJoiners:		botUserJoiners,
	}

	mapper := common.NewMapper(mapperConfig)
	mapper.Run()
	mapper.Stop()
}
