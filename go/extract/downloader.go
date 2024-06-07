package extract

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type Downloader interface {
	Download(output chan<- map[string]interface{}) error
}

type URLDownloaderConfig struct {
	Url string `mapstructure:"url,omitempty"`
}

type URLDownloader struct {
	config URLDownloaderConfig
}

func NewURLDownloader(config URLDownloaderConfig) *URLDownloader {
	return &URLDownloader{config: config}
}

func (d *URLDownloader) Download(output chan<- map[string]interface{}) error {
	resp, err := http.Get(d.config.Url)
	if err != nil {
		return fmt.Errorf("failed to fetch data: %w", err)
	}

	defer resp.Body.Close()

	decoder := json.NewDecoder(resp.Body)
	if _, err := decoder.Token(); err != nil {
		return fmt.Errorf("failed to read start of JSON array: %w", err)
	}

	for decoder.More() {
		var item map[string]interface{}
		if err := decoder.Decode(&item); err != nil {
			return fmt.Errorf("failed to decode JSON item: %w", err)
		}
		output <- item
	}

	if _, err := decoder.Token(); err != nil {
		return fmt.Errorf("failed to read end of JSON array: %w", err)
	}

	return nil
}
