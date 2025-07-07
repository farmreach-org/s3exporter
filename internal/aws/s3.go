package aws

import (
	"context"
	"fmt"
	// "io"
	"log"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	appConfig "sava-s3-export/internal/config"
)

// S3Client wraps the AWS S3 client
type S3Client struct {
	client     *s3.Client
	downloader *manager.Downloader
	bucket     string
	prefix     string
}

// NewS3Client creates a new S3 client
func NewS3Client(cfg *appConfig.Config) (*S3Client, error) {
	awsCfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(cfg.AWS_REGION),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AWS_ACCESS_KEY_ID, cfg.AWS_SECRET_ACCESS_KEY, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg)
	downloader := manager.NewDownloader(client)

	return &S3Client{
		client:     client,
		downloader: downloader,
		bucket:     cfg.S3_BUCKET,
		prefix:     cfg.S3_PREFIX,
	}, nil
}

// ListFiles lists all files in the S3 bucket with the given prefix
func (c *S3Client) ListFiles(ctx context.Context) ([]types.Object, error) {
	var files []types.Object
	paginator := s3.NewListObjectsV2Paginator(c.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(c.prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get page from S3: %w", err)
		}
		files = append(files, page.Contents...)
	}

	return files, nil
}

// DownloadFile downloads a file from S3 to the local filesystem
func (c *S3Client) DownloadFile(ctx context.Context, key, localPath string) error {
	// Ensure the directory exists
	dir := filepath.Dir(localPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Create the file
	file, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", localPath, err)
	}
	defer file.Close()

	_, err = c.downloader.Download(ctx, file, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		return fmt.Errorf("failed to download file %s: %w", key, err)
	}

	log.Printf("Successfully downloaded %s to %s", key, localPath)
	return nil
}