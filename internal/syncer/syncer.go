package syncer

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"
	// "time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"sava-s3-export/internal/aws"
	"sava-s3-export/internal/config"
	"sava-s3-export/internal/database"
)

// Syncer orchestrates the S3 sync process
type Syncer struct {
	s3Client *aws.S3Client
	db       *database.ParquetDB
	cfg      *config.Config
}

// NewSyncer creates a new Syncer
func NewSyncer(cfg *config.Config) (*Syncer, error) {
	s3Client, err := aws.NewS3Client(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}

	db, err := database.NewParquetDB(cfg.DB_PATH)
	if err != nil {
		return nil, fmt.Errorf("failed to create database: %w", err)
	}

	log.Println("Syncer initialized successfully.")
	return &Syncer{
		s3Client: s3Client,
		db:       db,
		cfg:      cfg,
	}, nil
}

// Run starts the sync process
func (s *Syncer) Run(ctx context.Context) error {
	log.Println("Starting S3 sync process...")

	// 1. List all files from S3
	s3Files, err := s.s3Client.ListFiles(ctx)
	if err != nil {
		return fmt.Errorf("failed to list S3 files: %w", err)
	}
	log.Printf("Found %d files in S3", len(s3Files))

	// 2. Get the current state from the local database
	localRecords, err := s.db.ReadAllRecords(ctx)
	if err != nil {
		return fmt.Errorf("failed to read local database: %w", err)
	}
	log.Printf("Found %d records in the local database", len(localRecords))

	// 3. Determine which files to download
	filesToDownload := s.getFilesToDownload(s3Files, localRecords)
	if len(filesToDownload) == 0 {
		log.Println("All files are up to date. Nothing to download.")
		return nil
	}
	log.Printf("Found %d files to download", len(filesToDownload))

	// 4. Download files concurrently
	var wg sync.WaitGroup
	downloadQueue := make(chan types.Object, len(filesToDownload))

	// Start worker goroutines
	numWorkers := 10 // Number of concurrent downloads
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go s.downloadWorker(ctx, &wg, downloadQueue)
	}

	// Add files to the download queue
	for _, file := range filesToDownload {
		downloadQueue <- file
	}
	close(downloadQueue)

	// Wait for all downloads to complete
	wg.Wait()

	log.Println("S3 sync process completed successfully.")
	return nil
}

// getFilesToDownload compares S3 files with local records to find what needs downloading
func (s *Syncer) getFilesToDownload(s3Files []types.Object, localRecords map[string]database.FileRecord) []types.Object {
	var toDownload []types.Object
	for _, s3File := range s3Files {
		key := *s3File.Key
		if record, exists := localRecords[key]; exists {
			// File exists locally, check if it has been modified
			if record.ETag != *s3File.ETag {
				toDownload = append(toDownload, s3File)
			}
		} else {
			// File does not exist locally
			toDownload = append(toDownload, s3File)
		}
	}
	return toDownload
}

// downloadWorker is a worker goroutine that downloads files from a channel
func (s *Syncer) downloadWorker(ctx context.Context, wg *sync.WaitGroup, queue <-chan types.Object) {
	defer wg.Done()
	for file := range queue {
		key := *file.Key
		localPath := filepath.Join(s.cfg.LOCAL_DIR, strings.TrimPrefix(key, s.cfg.S3_PREFIX))

		err := s.s3Client.DownloadFile(ctx, key, localPath)
		if err != nil {
			log.Printf("Failed to download %s: %v", key, err)
			// Optionally, update the database with a "failed" status
			s.db.UpdateSyncStatus(key, *file.ETag, localPath, "failed", *file.LastModified)
			continue
		}

		// Update the database with "downloaded" status
		err = s.db.UpdateSyncStatus(key, *file.ETag, localPath, "downloaded", *file.LastModified)
		if err != nil {
			log.Printf("Failed to update database for %s: %v", key, err)
		}
	}
}