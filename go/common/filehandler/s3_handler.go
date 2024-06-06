package filehandler

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
)

type S3Handler struct {
	ctx        context.Context
	client     *s3.Client
	bucketName string
}

var _ Handler = &S3Handler{}

func NewS3Handler(ctx context.Context, client *s3.Client, bucketName string) *S3Handler {
	return &S3Handler{
		ctx:        ctx,
		client:     client,
		bucketName: bucketName,
	}
}

func (s *S3Handler) Write(data []byte, fileName string) error {
	size := int64(len(data))
	file := &s3.PutObjectInput{
		Bucket:        aws.String(s.bucketName),
		Key:           aws.String(fileName),
		Body:          bytes.NewReader(data),
		ContentLength: &size,
		ContentType:   aws.String("text/plain"),
	}
	_, err := s.client.PutObject(s.ctx, file)
	if err != nil {
		return fmt.Errorf("could not upload file: %w", err)
	}

	return nil
}

func (s *S3Handler) Read(fileName string) ([]byte, error) {
	fileObject := &s3.GetObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(fileName),
	}

	file, err := s.client.GetObject(s.ctx, fileObject)
	if err != nil {
		return nil, fmt.Errorf("could not download file: %w", err)
	}
	defer file.Body.Close()

	data, err := io.ReadAll(file.Body)
	if err != nil {
		return nil, fmt.Errorf("could not read file data: %w", err)
	}

	return data, nil
}

func (s *S3Handler) List() ([]string, error) {
	info := &s3.ListObjectsInput{
		Bucket: aws.String(s.bucketName),
	}

	data, err := s.client.ListObjects(s.ctx, info)
	if err != nil {
		return nil, fmt.Errorf("could not list objects: %w", err)
	}

	keys := make([]string, 0)
	for _, content := range data.Contents {
		keys = append(keys, aws.ToString(content.Key))
	}

	return keys, nil
}

func (s *S3Handler) Delete(fileName string) error {
	info := &s3.DeleteObjectInput{
		Bucket: &s.bucketName,
		Key:    &fileName,
	}

	_, err := s.client.DeleteObject(s.ctx, info)
	if err != nil {
		return fmt.Errorf("could not delete object: %w", err)
	}

	return nil
}
