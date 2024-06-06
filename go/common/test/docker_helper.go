package test

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"testing"
)

func SetupLocalStack(t *testing.T) (context.Context, *s3.Client, string, func()) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "localstack/localstack",
		ExposedPorts: []string{"4566/tcp", "4572/tcp"},
		Env: map[string]string{
			"SERVICES": "s3",
		},
		WaitingFor: wait.ForListeningPort("4566/tcp"),
	}

	localstack, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("Failed to start localstack container: %s", err)
	}

	host, err := localstack.Host(ctx)
	if err != nil {
		t.Fatalf("Failed to get localstack container host: %s", err)
	}

	port, err := localstack.MappedPort(ctx, "4566/tcp")
	if err != nil {
		t.Fatalf("Failed to get localstack container port: %s", err)
	}

	endpoint := fmt.Sprintf("http://%s:%s", host, port.Port())

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("Failed to load AWS config: %s", err)
	}

	region := "eu-west-1"
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.Region = region
		o.BaseEndpoint = &endpoint
		o.UsePathStyle = true
	})

	teardown := func() {
		err := localstack.Terminate(ctx)
		if err != nil {
			t.Fatalf("Failed to terminate localstack container: %s", err)
		}
	}

	return ctx, client, region, teardown
}
