package main

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/LaCumbancha/reviews-analysis/nodes/mappers/city-business/common"
	"github.com/LaCumbancha/reviews-analysis/nodes/mappers/city-business/utils"

	log "github.com/sirupsen/logrus"
	logb "github.com/LaCumbancha/reviews-analysis/nodes/mappers/city-business/logger"
)

func InitConfig() (*viper.Viper, *viper.Viper, error) {
	configEnv := viper.New()

	// Configure viper to read env variables with the CITBIZMAP prefix
	configEnv.AutomaticEnv()
	configEnv.SetEnvPrefix("citbizmap")

	// Add env variables supported
	configEnv.BindEnv("instance")
	configEnv.BindEnv("rabbitmq", "ip")
	configEnv.BindEnv("rabbitmq", "port")
	configEnv.BindEnv("businesses", "inputs")
	configEnv.BindEnv("funcit", "joiners")
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

	businessesInputs := utils.GetConfigInt(configEnv, configFile, "businesses_inputs")
	
	if businessesInputs == 0 {
		log.Fatalf("BusinessesInputs variable missing")
	}

	funcitJoiners := utils.GetConfigInt(configEnv, configFile, "funcit_joiners")
	
	if funcitJoiners == 0 {
		log.Fatalf("FuncitJoiner variable missing")
	}

	mapperConfig := common.MapperConfig {
		Instance:			instance,
		RabbitIp:			rabbitIp,
		RabbitPort:			rabbitPort,
		BusinessesInputs:		businessesInputs,
		FuncitJoiners:		funcitJoiners,
	}

	// Initializing custom logger.
	logBulkRate := int(utils.GetConfigInt(configEnv, configFile, "log_bulk_rate")/5)
	
	if logBulkRate == 0 {
		log.Fatalf("LogBulkRate variable missing")
	}

	logb.Instance().SetBulkRate(logBulkRate)

	// Initializing mapper.
	mapper := common.NewMapper(mapperConfig)
	mapper.Run()
	mapper.Stop()
}
