package main

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/LaCumbancha/reviews-analysis/cmd/common/utils"
	"github.com/LaCumbancha/reviews-analysis/cmd/nodes/prettiers/funniest-cities/core"

	log "github.com/sirupsen/logrus"
)

func InitConfig() (*viper.Viper, *viper.Viper, error) {
	configEnv := viper.New()

	// Configure viper to read env variables with the FUNCITFIL prefix
	configEnv.AutomaticEnv()
	configEnv.SetEnvPrefix("funcitpre")

	// Add env variables supported
	configEnv.BindEnv("rabbitmq", "ip")
	configEnv.BindEnv("rabbitmq", "port")
	configEnv.BindEnv("funcit", "filters")
	configEnv.BindEnv("top", "size")
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

	rabbitIp := utils.GetConfigString(configEnv, configFile, "rabbitmq_ip")
	rabbitPort := utils.GetConfigString(configEnv, configFile, "rabbitmq_port")
	funcitFilters := utils.GetConfigInt(configEnv, configFile, "funcit_filters")
	topSize := utils.GetConfigInt(configEnv, configFile, "top_size")

	filterConfig := core.FilterConfig {
		RabbitIp:			rabbitIp,
		RabbitPort:			rabbitPort,
		FuncitFilters:		funcitFilters,
		TopSize:			topSize,
	}

	filter := core.NewFilter(filterConfig)
	filter.Run()
	filter.Stop()
}
