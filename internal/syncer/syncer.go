package syncer

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"golang.org/x/time/rate"

	"sava-s3-export/internal/aws"
	"sava-s3-export/internal/config"
	"sava-s3-export/internal/database"
)

// Syncer orchestrates the S3 sync process
type Syncer struct {
	s3Client    *aws.S3Client
	db          *database.ParquetDB
	cfg         *config.Config
	rateLimiter *rate.Limiter
	progress    *ProgressTracker
}

// NewSyncer creates a new Syncer
func NewSyncer(cfg *config.Config) (*Syncer, error) {
	s3Client, err := aws.NewS3Client(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}

	db, err := database.NewParquetDB(cfg.DB_PATH, cfg.BATCH_SIZE)
	if err != nil {
		return nil, fmt.Errorf("failed to create database: %w", err)
	}

	rateLimiter := rate.NewLimiter(rate.Limit(cfg.RATE_LIMIT_PER_SEC), cfg.RATE_LIMIT_PER_SEC)
	progress := NewProgressTracker()

	log.Println("Syncer initialized successfully.")
	return &Syncer{
		s3Client:    s3Client,
		db:          db,
		cfg:         cfg,
		rateLimiter: rateLimiter,
		progress:    progress,
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
	s.progress.Start(len(filesToDownload))
	defer s.progress.Finish()

	var wg sync.WaitGroup
	downloadQueue := make(chan types.Object, len(filesToDownload))

	// Start worker goroutines with configurable concurrency
	numWorkers := s.cfg.MAX_WORKERS
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

	// Flush any remaining batch updates
	if err := s.db.FlushBatch(); err != nil {
		log.Printf("Failed to flush final batch: %v", err)
	}

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
		// Rate limiting
		if err := s.rateLimiter.Wait(ctx); err != nil {
			log.Printf("Rate limiter context cancelled: %v", err)
			return
		}

		key := *file.Key
		localPath := filepath.Join(s.cfg.LOCAL_DIR, strings.TrimPrefix(key, s.cfg.S3_PREFIX))

		err := s.s3Client.DownloadFile(ctx, key, localPath)
		if err != nil {
			log.Printf("Failed to download %s: %v", key, err)
			// Use batch update for failed status
			s.db.BatchUpdate(key, *file.ETag, localPath, "failed", *file.LastModified)
			s.progress.IncrementFailed()
			continue
		}

		// Use batch update for downloaded status
		err = s.db.BatchUpdate(key, *file.ETag, localPath, "downloaded", *file.LastModified)
		if err != nil {
			log.Printf("Failed to update database for %s: %v", key, err)
		}
		s.progress.IncrementSuccess()
	}
}

// ProgressTracker tracks download progress
type ProgressTracker struct {
	total     int
	success   int
	failed    int
	startTime time.Time
	mu        sync.Mutex
}

// NewProgressTracker creates a new progress tracker
func NewProgressTracker() *ProgressTracker {
	return &ProgressTracker{}
}

// Start initializes the progress tracker
func (p *ProgressTracker) Start(total int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.total = total
	p.success = 0
	p.failed = 0
	p.startTime = time.Now()
	log.Printf("Starting download of %d files", total)
}

// IncrementSuccess increments successful downloads
func (p *ProgressTracker) IncrementSuccess() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.success++
	p.logProgress()
}

// IncrementFailed increments failed downloads
func (p *ProgressTracker) IncrementFailed() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failed++
	p.logProgress()
}

// logProgress logs current progress
func (p *ProgressTracker) logProgress() {
	completed := p.success + p.failed
	if completed%100 == 0 || completed == p.total {
		elapsed := time.Since(p.startTime)
		rate := float64(completed) / elapsed.Seconds()
		log.Printf("Progress: %d/%d files (%.1f%%), Success: %d, Failed: %d, Rate: %.1f files/sec", 
			completed, p.total, float64(completed)*100/float64(p.total), p.success, p.failed, rate)
	}
}

// Finish logs final statistics
func (p *ProgressTracker) Finish() {
	p.mu.Lock()
	defer p.mu.Unlock()
	elapsed := time.Since(p.startTime)
	rate := float64(p.success+p.failed) / elapsed.Seconds()
	log.Printf("Download completed in %v: %d successful, %d failed, %.1f files/sec", 
		elapsed, p.success, p.failed, rate)
}