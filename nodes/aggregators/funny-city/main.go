package main

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/LaCumbancha/reviews-analysis/nodes/aggregators/funny-city/common"
	"github.com/LaCumbancha/reviews-analysis/nodes/aggregators/funny-city/utils"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/nodes/aggregators/funny-city/logger"
)

func InitConfig() (*viper.Viper, *viper.Viper, error) {
	configEnv := viper.New()

	// Configure viper to read env variables with the FUNCITAGG prefix
	configEnv.AutomaticEnv()
	configEnv.SetEnvPrefix("funcitagg")

	// Add env variables supported
	configEnv.BindEnv("instance")
	configEnv.BindEnv("rabbitmq", "ip")
	configEnv.BindEnv("rabbitmq", "port")
	configEnv.BindEnv("input", "topic")
	configEnv.BindEnv("funcit", "joiners")
	configEnv.BindEnv("funcit", "filters")
	configEnv.BindEnv("output", "bulk", "size")
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

	inputTopic := utils.GetConfigString(configEnv, configFile, "input_topic")
	
	if inputTopic == "" {
		log.Fatalf("InputTopic variable missing")
	}

	funcitJoiners := utils.GetConfigInt(configEnv, configFile, "funcit_joiners")
	
	if funcitJoiners == 0 {
		log.Fatalf("FuncitJoiners variable missing")
	}

	funcitFilters := utils.GetConfigInt(configEnv, configFile, "funcit_filters")
	
	if funcitFilters == 0 {
		log.Fatalf("FuncitFilters variable missing")
	}

	outputBulkSize := utils.GetConfigInt(configEnv, configFile, "output_bulk_size")
	
	if outputBulkSize == 0 {
		log.Fatalf("OutputBulkSize variable missing")
	}

	aggregatorConfig := common.AggregatorConfig {
		Instance:			instance,
		RabbitIp:			rabbitIp,
		RabbitPort:			rabbitPort,
		InputTopic: 		inputTopic,
		FuncitFilters:		funcitFilters,
		FuncitJoiners:		funcitJoiners,
		OutputBulkSize:		outputBulkSize,
	}

	// Initializing custom logger.
	logBulkRate := utils.GetConfigInt(configEnv, configFile, "log_bulk_rate")
	
	if logBulkRate == 0 {
		log.Fatalf("LogBulkRate variable missing")
	}

	logb.Instance().SetBulkRate(logBulkRate)

	// Initializing aggregator.
	aggregator := common.NewAggregator(aggregatorConfig)
	aggregator.Run()
	aggregator.Stop()
}
