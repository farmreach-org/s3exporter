# S3 Sava Export

This application syncs files from an AWS S3 prefix to a local directory. It uses a local Parquet file to keep track of the sync status of each file.

## Configuration

The application can be configured via a `.env` file in the root of the project. If no `.env` file is found, it will use the hardcoded values in `internal/config/config.go`.

Create a `.env` file with the following content:

```
AWS_ACCESS_KEY_ID=YOUR_AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY=YOUR_AWS_SECRET_ACCESS_KEY
AWS_REGION=us-east-1
S3_BUCKET=your-s3-bucket-name
S3_PREFIX=your-s3-prefix/
LOCAL_DIR=./data
DB_PATH=./s3_sync_status.parquet
```

## Build

Before building, you need to fetch the dependencies:

```bash
go get github.com/joho/godotenv
go get github.com/aws/aws-sdk-go-v2/config
go get github.com/aws/aws-sdk-go-v2/credentials
go get github.com/aws/aws-sdk-go-v2/service/s3
go get github.com/aws/aws-sdk-go-v2/feature/s3/manager
go get github.com/xitongsys/parquet-go-source/local
go get github.com/xitongsys/parquet-go/writer
go get github.com/xitongsys/parquet-go/reader
go mod tidy
```

Then, you can build the application for your desired platform:

### Linux

```bash
GOOS=linux GOARCH=amd64 go build -o sava-s3-export-linux ./cmd/sava-s3-export
```

### Windows

```bash
GOOS=windows GOARCH=amd64 go build -o sava-s3-export-windows.exe ./cmd/sava-s3-export
```

## Usage

Once built, you can run the application from your terminal:

```bash
./sava-s3-export-linux
```

Or on Windows:

```bash
sava-s3-export-windows.exe