package backends

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	log "github.com/sirupsen/logrus"
	"github.com/thediveo/enumflag/v2"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go-source/mem"
	"github.com/xitongsys/parquet-go-source/s3v2"
	"github.com/xitongsys/parquet-go/source"
)

type StorageBackend enumflag.Flag

const (
	Local StorageBackend = iota
	Memory

	S3
)

// s3Client is a cached S3 client for reuse
var s3Client *s3.Client

// initS3Client initializes the S3 client with proper credential chain
func initS3Client(ctx context.Context) (*s3.Client, error) {
	if s3Client != nil {
		return s3Client, nil
	}

	// Load AWS config using default credential chain (supports IRSA, env vars, etc.)
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	s3Client = s3.NewFromConfig(cfg)
	log.Infof("initialized S3 client with region: %s", cfg.Region)
	return s3Client, nil
}

func ConstructBackendForFile( //nolint:ireturn // this is fine
	root,
	file string,
	backend StorageBackend,
) (source.ParquetFile, error) {
	switch backend {
	case Local:
		fullPath, err := filepath.Abs(fmt.Sprintf("%s/%s", root, file))
		if err != nil {
			return nil, fmt.Errorf("can't construct local path %s/%s: %w", root, file, err)
		}

		log.Infof("using local backend, writing to %s", fullPath)
		if err := os.MkdirAll(filepath.Dir(fullPath), 0750); err != nil {
			return nil, fmt.Errorf("can't create directory: %w", err)
		}

		fw, err := local.NewLocalFileWriter(fullPath)
		if err != nil {
			return nil, fmt.Errorf("can't create local file writer: %w", err)
		}

		return fw, nil

	case Memory:
		log.Warnf("using in-memory backend, this is intended for testing only!")
		fullPath, err := filepath.Abs(fmt.Sprintf("%s/%s", root, file))
		if err != nil {
			return nil, fmt.Errorf("can't construct local path %s/%s: %w", root, file, err)
		}

		fw, err := mem.NewMemFileWriter(fullPath, nil)
		if err != nil {
			return nil, fmt.Errorf("can't create in-memory writer: %w", err)
		}

		return fw, nil

	case S3:
		// root format: "bucket-name/prefix" or just "bucket-name"
		// file format: "path/to/file.parquet"
		// We need to split root into bucket and prefix, then combine prefix with file for the key
		bucket := root
		key := file
		if idx := strings.Index(root, "/"); idx != -1 {
			bucket = root[:idx]
			prefix := root[idx+1:]
			key = prefix + "/" + file
		}

		log.Infof("using S3 backend, writing to s3://%s/%s", bucket, key)

		ctx := context.Background()
		client, err := initS3Client(ctx)
		if err != nil {
			return nil, fmt.Errorf("can't initialize S3 client: %w", err)
		}

		// Pass the properly configured S3 client to the writer
		fw, err := s3v2.NewS3FileWriterWithClient(ctx, client, bucket, key, nil)
		if err != nil {
			return nil, fmt.Errorf("can't create S3 writer: %w", err)
		}

		return fw, nil
	}

	return nil, nil
}
