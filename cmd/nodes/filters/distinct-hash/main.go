package main

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/LaCumbancha/reviews-analysis/cmd/common/utils"
	"github.com/LaCumbancha/reviews-analysis/cmd/nodes/filters/distinct-hash/core"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/cmd/common/logger"
)

func InitConfig() (*viper.Viper, *viper.Viper, error) {
	configEnv := viper.New()

	// Configure viper to read env variables with the DISHASHFIL prefix
	configEnv.AutomaticEnv()
	configEnv.SetEnvPrefix("dishashfil")

	// Add env variables supported
	configEnv.BindEnv("instance")
	configEnv.BindEnv("rabbitmq", "ip")
	configEnv.BindEnv("rabbitmq", "port")
	configEnv.BindEnv("workers", "pool")
	configEnv.BindEnv("dishash", "aggregators")
	configEnv.BindEnv("dishash", "joiners")
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
	rabbitIp := utils.GetConfigString(configEnv, configFile, "rabbitmq_ip")
	rabbitPort := utils.GetConfigString(configEnv, configFile, "rabbitmq_port")
	workersPool := utils.GetConfigInt(configEnv, configFile, "workers_pool")
	dishashAggregators := utils.GetConfigInt(configEnv, configFile, "dishash_aggregators")
	dishashJoiners := utils.GetConfigInt(configEnv, configFile, "dishash_joiners")

	filterConfig := core.FilterConfig {
		Instance:				instance,
		RabbitIp:				rabbitIp,
		RabbitPort:				rabbitPort,
		WorkersPool:			workersPool,
		DishashAggregators:		dishashAggregators,
		DishashJoiners:			dishashJoiners,
	}

	// Initializing custom logger.
	logBulkRate := utils.GetConfigInt(configEnv, configFile, "log_bulk_rate")
	logb.Instance().SetBulkRate(logBulkRate)

	// Initializing filter.
	filter := core.NewFilter(filterConfig)
	filter.Run()
	filter.Stop()
}
