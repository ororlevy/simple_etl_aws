package extract

import (
	"encoding/json"
	"github.com/magiconair/properties/assert"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestURLDownloader(t *testing.T) {
	mockResponse := []map[string]interface{}{
		{"name": "Alice", "age": 30},
		{"name": "Bob", "age": 25},
	}
	mockResponseBytes, err := json.Marshal(mockResponse)
	if err != nil {
		t.Fatalf("failed to marshal mock response: %v", err)
	}

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write(mockResponseBytes)
		if err != nil {
			t.Fatalf("failed to write mock response: %v", err)
		}
	}))
	defer mockServer.Close()

	// Initialize the URLDownloader with the mock server URL
	downloader := &URLDownloader{
		config: URLDownloaderConfig{
			Url: mockServer.URL,
		},
	}

	// Create a channel to receive the output
	output := make(chan map[string]interface{})

	go func() {
		if err := downloader.Download(output); err != nil {
			t.Errorf("Download failed: %v", err)
			return
		}
		close(output)
	}()

	var results []map[string]interface{}
	for item := range output {
		results = append(results, item)
	}

	assert.Equal(t, len(results), len(mockResponse))
}
