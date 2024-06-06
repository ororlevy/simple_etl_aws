package extract

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/magiconair/properties/assert"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"simple_etl_aws/common/filehandler"
	"simple_etl_aws/common/test"
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

	ctx, client, region, teardown := test.SetupLocalStack(t)

	defer teardown()

	fileSystemHandler := filehandler.NewFileSystemHandler(tempDir)
	bucketName := "testbucket"
	s3Handler := filehandler.NewS3Handler(ctx, client, bucketName)

	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: &bucketName,
		CreateBucketConfiguration: &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(region),
		},
	})
	if err != nil {
		t.Fatalf("could not create bucket: %v", err)
	}

	//nolint: dogsled
	_, filename, _, _ := runtime.Caller(0)
	projectRoot := filepath.Dir(filename)

	data := readFile(t, filehandler.NewFileSystemHandler(projectRoot), testDataPath)

	if err != nil {
		t.Fatalf("could not load test data: %v", err)
	}

	testCases := []struct {
		name              string
		responseTime      int
		config            DumpConfig
		handler           filehandler.Handler
		expectedFilecount int
		expectedResult    []map[string]interface{}
	}{
		{
			"file system with basic flow",
			0,
			DumpConfig{TimeLimitInMilliseconds: 1000, SizeLimitInMB: 1},
			fileSystemHandler,
			1, data,
		},
		// we change the response time to have different file name
		{
			"file system with size limit",
			100,
			DumpConfig{TimeLimitInMilliseconds: 1000, SizeLimitInMB: 0},
			fileSystemHandler,
			2,
			data,
		},
		{
			"file system with time limit",
			1000,
			DumpConfig{TimeLimitInMilliseconds: 500, SizeLimitInMB: 1},
			fileSystemHandler,
			2,
			data,
		},
		{
			"s3 with basic flow",
			0,
			DumpConfig{TimeLimitInMilliseconds: 1000, SizeLimitInMB: 1},
			s3Handler,
			1, data,
		},
		// we change the response time to have different file name
		{
			"s3 with size limit",
			100,
			DumpConfig{TimeLimitInMilliseconds: 1000, SizeLimitInMB: 0},
			s3Handler,
			2,
			data,
		},
		{
			"s3 with time limit",
			1000,
			DumpConfig{TimeLimitInMilliseconds: 500, SizeLimitInMB: 1},
			s3Handler,
			2,
			data,
		},
	}

	for _, tcase := range testCases {
		t.Run(tcase.name, func(t *testing.T) {
			downloader := dummyDownloader{
				data:                       data,
				responseTimeInMilliseconds: time.Duration(tcase.responseTime) * time.Millisecond,
			}

			dumper := NewDumper(context.Background(), tcase.config, &downloader, tcase.handler)

			err := dumper.Run()
			if err != nil {
				t.Fatalf("Could not dump files: %v", err)
			}

			files, err := tcase.handler.List()
			if err != nil {
				t.Fatalf("could not read files %v", err)
			}

			assert.Equal(t, len(files), tcase.expectedFilecount)

			result := readAllfilesAndConcat(t, files, tcase.handler)

			assert.Equal(t, reflect.DeepEqual(result, tcase.expectedResult), true)

			cleanFiles(t, files, tcase.handler)
		})
	}

}

func readAllfilesAndConcat(t *testing.T, files []string, handler filehandler.Handler) []map[string]interface{} {

	result := make([]map[string]interface{}, 0)

	for _, file := range files {
		items := readFile(t, handler, file)
		result = append(result, items...)
	}

	return result
}

func readFile(t *testing.T, handler filehandler.Handler, file string) []map[string]interface{} {
	data, err := handler.Read(file)
	if err != nil {
		t.Fatalf("failed, can't read result files: %v", err)
	}

	var items []map[string]interface{}
	if err := json.Unmarshal(data, &items); err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}
	return items
}

func cleanFiles(t *testing.T, files []string, handler filehandler.Handler) {
	for _, file := range files {
		err := handler.Delete(file)
		if err != nil {
			t.Fatalf("could not clean file %v", err)
		}
	}
}
