package extract

import (
	"fmt"
	"github.com/spf13/viper"
)

type Config struct {
	DumperConfig     DumpConfig          `mapstructure:"dumper,omitempty"`
	DownloaderConfig URLDownloaderConfig `mapstructure:"downloader,omitempty"`
	BucketName       string              `mapstructure:"bucket,omitempty"`
}

func ReadConfig(path, fileName string) (*Config, error) {
	viper.SetConfigName(fileName)
	viper.AddConfigPath(path)

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
