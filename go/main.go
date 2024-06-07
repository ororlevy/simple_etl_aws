package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rs/zerolog/log"
)

func main() {
	ctx := context.Background()

	cfg, err := ReadConfig()
	if err != nil {
		log.Error().Msgf("execution failed: %v", err)
		return
	}

	client, err := createS3Client(ctx, cfg)
	if err != nil {
		log.Error().Msgf("execution failed: %v", err)
		return
	}

	runner, err := createRunner(ctx, cfg.LambdaType, client)
	if err != nil {
		log.Error().Msgf("execution failed: %v", err)
		return
	}

	lambda.Start(runner)
}

func createS3Client(ctx context.Context, cfg *Config) (*s3.Client, error) {
	cfgLoadOptions := make([]func(*config.LoadOptions) error, 0)

	if cfg.Credentials != nil {
		cfgLoadOptions =
			append(cfgLoadOptions, config.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(
					cfg.Credentials.Key,
					cfg.Credentials.Secret,
					"")))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, cfgLoadOptions...)
	if err != nil {
		return nil, fmt.Errorf("could not create AWS config: %w", err)
	}

	s3Opt := func(o *s3.Options) {}

	if cfg.Endpoint != "" {
		s3Opt = func(o *s3.Options) {
			o.BaseEndpoint = &cfg.Endpoint
			o.UsePathStyle = true
		}
	}

	region := "eu-west-1"
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.Region = region
	}, s3Opt)

	return client, nil
}
