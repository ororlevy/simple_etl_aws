package extract

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/magiconair/properties/assert"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"simple_etl_aws/common/filehandler"
	"testing"
	"time"
)

const (
	testDataPath = "/resources/test_data.json"
)

type dummyDownloader struct {
	data                       []map[string]interface{}
	responseTimeInMilliseconds time.Duration
}

var _ Downloader = &dummyDownloader{}

func (dm *dummyDownloader) Download(output chan<- map[string]interface{}) error {
	for _, item := range dm.data {
		time.Sleep(dm.responseTimeInMilliseconds)
		output <- item
	}

	return nil
}

func TestDumper(t *testing.T) {

	tempDir, err := os.MkdirTemp("", "test")
	if err != nil {
		t.Errorf("could not create temp file %v", err)
	}

	defer os.RemoveAll(tempDir)

	//nolint: dogsled
	_, filename, _, _ := runtime.Caller(0)
	projectRoot := filepath.Dir(filename)

	data := readFile(t, filepath.Join(projectRoot, testDataPath))

	testCases := []struct {
		name              string
		responseTime      int
		config            DumpConfig
		expectedFilecount int
		expectedResult    []map[string]interface{}
	}{
		{"basic flow", 0, DumpConfig{TimeLimitInMilliseconds: 1000, SizeLimitInMB: 1}, 1, data},
		// we change the response time to have different file name
		{"size limit", 100, DumpConfig{TimeLimitInMilliseconds: 1000, SizeLimitInMB: 0}, 2, data},
		{"time limit", 1000, DumpConfig{TimeLimitInMilliseconds: 500, SizeLimitInMB: 1}, 2, data},
	}

	fileSystemHandler := filehandler.NewFileSystemHandler(tempDir)

	for _, tcase := range testCases {
		t.Run(tcase.name, func(t *testing.T) {
			downloader := dummyDownloader{
				data:                       data,
				responseTimeInMilliseconds: time.Duration(tcase.responseTime) * time.Millisecond,
			}

			dumper := NewDumper(context.Background(), tcase.config, &downloader, fileSystemHandler)

			err := dumper.Run()
			if err != nil {
				t.Fatalf("Could not dump files: %v", err)
			}

			files, err := os.ReadDir(tempDir)
			if err != nil {
				t.Fatalf("could not read files %v", err)
			}

			assert.Equal(t, len(files), tcase.expectedFilecount)

			result := readAllfilesAndConcat(t, tempDir, files)

			assert.Equal(t, reflect.DeepEqual(result, tcase.expectedResult), true)

			cleanFiles(t, tempDir, files)
		})
	}

}

func readFile(t *testing.T, path string) []map[string]interface{} {
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("could not load data %v", err)
	}

	var items []map[string]interface{}
	if err := json.Unmarshal(data, &items); err != nil {
		fmt.Printf(string(data))
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	return items
}

func readAllfilesAndConcat(t *testing.T, path string, files []fs.DirEntry) []map[string]interface{} {

	result := make([]map[string]interface{}, 0)

	for _, file := range files {
		result = append(result, readFile(t, filepath.Join(path, file.Name()))...)
	}

	return result
}

func cleanFiles(t *testing.T, path string, files []fs.DirEntry) {
	for _, file := range files {
		err := os.Remove(filepath.Join(path, file.Name()))
		if err != nil {
			t.Fatalf("could not clean file %v", err)
		}
	}
}
