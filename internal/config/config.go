package config

import (
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// Config holds the application configuration
type Config struct {
	AWS_ACCESS_KEY_ID     string
	AWS_SECRET_ACCESS_KEY string
	AWS_REGION            string
	S3_BUCKET             string
	S3_PREFIX             string
	LOCAL_DIR             string
	DB_PATH               string
	MAX_WORKERS           int
	BATCH_SIZE            int
	RATE_LIMIT_PER_SEC    int
}

// Load loads the configuration from a .env file or uses hardcoded defaults
func Load() *Config {
	// Load .env file if it exists
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using hardcoded defaults")
	}

	return &Config{
		AWS_ACCESS_KEY_ID:     getEnv("AWS_ACCESS_KEY_ID", "YOUR_AWS_ACCESS_KEY_ID"),
		AWS_SECRET_ACCESS_KEY: getEnv("AWS_SECRET_ACCESS_KEY", "YOUR_AWS_SECRET_ACCESS_KEY"),
		AWS_REGION:            getEnv("AWS_REGION", "us-east-1"),
		S3_BUCKET:             getEnv("S3_BUCKET", "your-s3-bucket-name"),
		S3_PREFIX:             getEnv("S3_PREFIX", "your-s3-prefix/"),
		LOCAL_DIR:             getEnv("LOCAL_DIR", "./data"),
		DB_PATH:               getEnv("DB_PATH", "./s3_sync_status.parquet"),
		MAX_WORKERS:           getEnvInt("MAX_WORKERS", 50),
		BATCH_SIZE:            getEnvInt("BATCH_SIZE", 100),
		RATE_LIMIT_PER_SEC:    getEnvInt("RATE_LIMIT_PER_SEC", 100),
	}
}

// getEnv retrieves an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

// getEnvInt retrieves an environment variable as integer or returns a default value
func getEnvInt(key string, defaultValue int) int {
	if value, exists := os.LookupEnv(key); exists {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}