package database

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

// FileRecord represents a single record in the Parquet database
type FileRecord struct {
	S3Key        string `parquet:"name=s3_key, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ETag         string `parquet:"name=etag, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	LastModified int64  `parquet:"name=last_modified, type=INT64"`
	SyncStatus   string `parquet:"name=sync_status, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	LocalPath    string `parquet:"name=local_path, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	LastSyncedAt int64  `parquet:"name=last_synced_at, type=INT64"`
}

// ParquetDB handles operations on the Parquet database file
type ParquetDB struct {
	path        string
	batchBuffer []FileRecord
	batchSize   int
}

// NewParquetDB creates a new ParquetDB instance
func NewParquetDB(path string, batchSize int) (*ParquetDB, error) {
	db := &ParquetDB{
		path:        path,
		batchBuffer: make([]FileRecord, 0, batchSize),
		batchSize:   batchSize,
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		log.Println("No database file found, creating a new one...")
		if err := db.createEmptyFile(); err != nil {
			return nil, fmt.Errorf("failed to create empty database file: %w", err)
		}
		log.Println("Successfully created new database file.")
	}
	return db, nil
}

// createEmptyFile creates an empty Parquet file with the correct schema
func (db *ParquetDB) createEmptyFile() error {
	fw, err := local.NewLocalFileWriter(db.path)
	if err != nil {
		return fmt.Errorf("failed to create local file writer: %w", err)
	}
	defer fw.Close()

	pw, err := writer.NewParquetWriter(fw, new(FileRecord), 4)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}
	pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	pw.PageSize = 8 * 1024              // 8K
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	if err := pw.WriteStop(); err != nil {
		return fmt.Errorf("failed to stop parquet writer: %w", err)
	}
	log.Printf("Successfully created empty Parquet file at %s", db.path)
	return nil
}

// ReadAllRecords reads all records from the Parquet file
func (db *ParquetDB) ReadAllRecords(ctx context.Context) (map[string]FileRecord, error) {
	fr, err := local.NewLocalFileReader(db.path)
	if err != nil {
		return nil, fmt.Errorf("failed to create local file reader: %w", err)
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, new(FileRecord), 4)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader: %w", err)
	}
	defer pr.ReadStop()

	numRows := pr.GetNumRows()
	records := make([]FileRecord, numRows)
	if err := pr.Read(&records); err != nil {
		return nil, fmt.Errorf("failed to read records: %w", err)
	}

	recordMap := make(map[string]FileRecord, numRows)
	for _, r := range records {
		recordMap[r.S3Key] = r
	}

	return recordMap, nil
}

// WriteRecords writes a slice of records to the Parquet file, overwriting existing content
func (db *ParquetDB) WriteRecords(records []FileRecord) error {
	fw, err := local.NewLocalFileWriter(db.path)
	if err != nil {
		return fmt.Errorf("failed to create local file writer: %w", err)
	}
	defer fw.Close()

	pw, err := writer.NewParquetWriter(fw, new(FileRecord), 4)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}
	defer pw.WriteStop()

	pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	pw.PageSize = 8 * 1024              // 8K
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for _, r := range records {
		if err := pw.Write(r); err != nil {
			return fmt.Errorf("failed to write record: %w", err)
		}
	}

	log.Printf("Successfully wrote %d records to %s", len(records), db.path)
	return nil
}

// UpdateSyncStatus updates the sync status of a given file
func (db *ParquetDB) UpdateSyncStatus(s3Key, etag, localPath, status string, lastModified time.Time) error {
	records, err := db.ReadAllRecords(context.Background())
	if err != nil {
		return fmt.Errorf("failed to read records for update: %w", err)
	}

	record, exists := records[s3Key]
	if !exists {
		record = FileRecord{S3Key: s3Key}
	}

	record.ETag = etag
	record.LocalPath = localPath
	record.SyncStatus = status
	record.LastModified = lastModified.Unix()
	record.LastSyncedAt = time.Now().Unix()
	records[s3Key] = record

	var recordSlice []FileRecord
	for _, r := range records {
		recordSlice = append(recordSlice, r)
	}

	return db.WriteRecords(recordSlice)
}

// BatchUpdate adds a record to the batch buffer
func (db *ParquetDB) BatchUpdate(s3Key, etag, localPath, status string, lastModified time.Time) error {
	record := FileRecord{
		S3Key:        s3Key,
		ETag:         etag,
		LocalPath:    localPath,
		SyncStatus:   status,
		LastModified: lastModified.Unix(),
		LastSyncedAt: time.Now().Unix(),
	}
	
	db.batchBuffer = append(db.batchBuffer, record)
	
	if len(db.batchBuffer) >= db.batchSize {
		return db.FlushBatch()
	}
	
	return nil
}

// FlushBatch writes all buffered records to the database
func (db *ParquetDB) FlushBatch() error {
	if len(db.batchBuffer) == 0 {
		return nil
	}
	
	existingRecords, err := db.ReadAllRecords(context.Background())
	if err != nil {
		return fmt.Errorf("failed to read existing records: %w", err)
	}
	
	for _, record := range db.batchBuffer {
		existingRecords[record.S3Key] = record
	}
	
	var recordSlice []FileRecord
	for _, r := range existingRecords {
		recordSlice = append(recordSlice, r)
	}
	
	if err := db.WriteRecords(recordSlice); err != nil {
		return fmt.Errorf("failed to write batch: %w", err)
	}
	
	log.Printf("Flushed batch of %d records to database", len(db.batchBuffer))
	db.batchBuffer = db.batchBuffer[:0]
	
	return nil
}