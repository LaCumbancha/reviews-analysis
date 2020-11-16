package main

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	log "github.com/sirupsen/logrus"

	"github.com/LaCumbancha/backup-server/backup-manager/utils"
	"github.com/LaCumbancha/backup-server/backup-manager/common"
	"github.com/LaCumbancha/backup-server/backup-manager/manager"
	"github.com/LaCumbancha/backup-server/backup-manager/scheduler"
)

func InitConfig() (*viper.Viper, *viper.Viper, error) {
	configEnv := viper.New()

	// Configure viper to read env variables with the BKPMNGR_ prefix
	configEnv.AutomaticEnv()
	configEnv.SetEnvPrefix("bkp")

	// Add env variables supported
	configEnv.BindEnv("storage")
	configEnv.BindEnv("manager", "port")
	configEnv.BindEnv("scheduler", "port")
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
		log.Fatalf("%s", err)
	}

	storagePath := utils.GetConfigValue(configEnv, configFile, "storage")
	
	if storagePath == "" {
		log.Fatalf("Storage variable missing")
	}

	managerPort := utils.GetConfigValue(configEnv, configFile, "manager_port")
	
	if managerPort == "" {
		log.Fatalf("Port variable missing")
	}

	schedulerPort := utils.GetConfigValue(configEnv, configFile, "scheduler_port")
	
	if schedulerPort == "" {
		log.Fatalf("Port variable missing")
	}

	backupStorageConfig := common.BackupStorageConfig {
		Path: 			storagePath,
	}

	backupStorage := common.NewBackupStorage(backupStorageConfig)
	backupStorage.BuildBackupStructure()

	backupSchedulerConfig := scheduler.BackupSchedulerConfig {
		Storage:		backupStorage,
	}

	backupScheduler := scheduler.NewBackupScheduler(backupSchedulerConfig)
	go backupScheduler.Run()

	managerConfig := manager.BackupManagerConfig {
		Port: 			managerPort,
		Storage: 		backupStorage,
	}

	backupManager := manager.NewBackupManager(managerConfig)
	backupManager.Run()
}
