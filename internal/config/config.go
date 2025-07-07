package config

import (
	"log"
	"os"

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
	}
}

// getEnv retrieves an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}