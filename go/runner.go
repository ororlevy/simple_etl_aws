package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"simple_etl_aws/common/filehandler"
	"simple_etl_aws/extract"
)

func createRunner(ctx context.Context, lamdaType LambdaType, client *s3.Client) (interface{}, error) {
	switch lamdaType {
	case Extract:
		dumper, err := createExtract(ctx, client)
		if err != nil {
			return nil, fmt.Errorf("could not create runner: %w", err)
		}

		return dumper.Run, nil
	}

	return nil, nil
}

func createExtract(ctx context.Context, client *s3.Client) (*extract.Dumper, error) {
	cfg, err := extract.ReadConfig(*configPathFlag, configFileName)
	if err != nil {
		return nil, fmt.Errorf("can't create extract: %w", err)
	}

	downloader := extract.NewURLDownloader(cfg.DownloaderConfig)
	s3Handler := filehandler.NewS3Handler(ctx, client, cfg.BucketName)

	return extract.NewDumper(ctx, cfg.DumperConfig, downloader, s3Handler), nil

}
