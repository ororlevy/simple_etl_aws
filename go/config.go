package main

import (
	"flag"
	"fmt"
	"github.com/spf13/viper"
)

var configPathFlag = flag.String("config", "resources/", "config file path")

const configFileName = "config"

type Config struct {
	LambdaType  LambdaType      `mapstructure:"type,omitempty"`
	Region      string          `mapstructure:"region,omitempty"`
	Endpoint    string          `mapstructure:"endpoint,omitempty"`
	Credentials *AWSCredentials `mapstructure:"credentials,omitempty"`
}

type AWSCredentials struct {
	Key    string `mapstructure:"key,omitempty"`
	Secret string `mapstructure:"secret,omitempty"`
}

type LambdaType string

const (
	Extract LambdaType = "extract"
	Load    LambdaType = "load"
)

func ReadConfig() (*Config, error) {
	flag.Parse()

	configPath := *configPathFlag

	viper.SetConfigName(configFileName)
	viper.AddConfigPath(configPath)

	err := viper.ReadInConfig()
	if err != nil {
		return nil, fmt.Errorf("could not read config file: %w", err)
	}

	var config = &Config{}

	err = viper.Unmarshal(config)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal config: %w", err)
	}

	return config, nil
}
