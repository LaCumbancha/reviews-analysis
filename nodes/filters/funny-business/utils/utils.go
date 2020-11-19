package utils

import (
	"path/filepath"
	"github.com/spf13/viper"
)

// Get configuration file's path structure. 
func GetConfigFile(configFileName string) (string, string, string) {
	path := filepath.Dir(configFileName)
	file := filepath.Base(configFileName)
	ctype := filepath.Ext(configFileName)[1:]

	return path, file, ctype
}

// Give precedence to environment variables over configuration file's
func GetConfigString(configEnv *viper.Viper, configFile *viper.Viper, key string) string {
	value := configEnv.GetString(key)
	if value == "" {
		value = configFile.GetString(key)
	}

	return value
}

// Give precedence to environment variables over configuration file's
func GetConfigInt(configEnv *viper.Viper, configFile *viper.Viper, key string) int {
	value := configEnv.GetInt(key)
	if value == 0 {
		value = configFile.GetInt(key)
	}

	return value
}
