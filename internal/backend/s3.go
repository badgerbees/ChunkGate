package backend

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const (
	defaultS3Timeout = 30 * time.Second
)

type S3Options struct {
	Endpoint     string
	Region       string
	Bucket       string
	AccessKey    string
	SecretKey    string
	SessionToken string
	Prefix       string
	Secure       bool
	PathStyle    bool
	Timeout      time.Duration
	MaxRetries   int
}

type S3Store struct {
	client     *minio.Client
	bucket     string
	prefix     string
	timeout    time.Duration
	maxRetries int
}

func NewS3Store(options S3Options) (*S3Store, error) {
	endpoint, secure, err := normalizeEndpoint(options.Endpoint, options.Secure)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(options.Bucket) == "" {
		return nil, fmt.Errorf("s3 bucket must not be empty")
	}
	if options.Timeout < 0 {
		return nil, fmt.Errorf("s3 timeout must be >= 0")
	}
	if options.MaxRetries < 0 {
		return nil, fmt.Errorf("s3 max retries must be >= 0")
	}
	timeout := options.Timeout
	if timeout == 0 {
		timeout = defaultS3Timeout
	}

	bucketLookup := minio.BucketLookupDNS
	if options.PathStyle {
		bucketLookup = minio.BucketLookupPath
	}
	client, err := minio.New(endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(options.AccessKey, options.SecretKey, options.SessionToken),
		Secure:       secure,
		Region:       options.Region,
		BucketLookup: bucketLookup,
		MaxRetries:   1,
	})
	if err != nil {
		return nil, fmt.Errorf("create s3 client: %w", err)
	}

	return &S3Store{
		client:     client,
		bucket:     options.Bucket,
		prefix:     cleanS3Prefix(options.Prefix),
		timeout:    timeout,
		maxRetries: options.MaxRetries,
	}, nil
}

func (s *S3Store) PutBlock(ctx context.Context, tenant string, hash string, data []byte) error {
	key, err := s.blockKey(tenant, hash)
	if err != nil {
		return err
	}
	err = s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.client.PutObject(ctx, s.bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
			ContentType: "application/octet-stream",
		})
		return err
	})
	return wrapS3Error("put block", key, err)
}

func (s *S3Store) GetBlock(ctx context.Context, tenant string, hash string) (io.ReadCloser, error) {
	key, err := s.blockKey(tenant, hash)
	if err != nil {
		return nil, err
	}
	attempts := s.maxRetries + 1
	if attempts < 1 {
		attempts = 1
	}
	var last error
	for attempt := 0; attempt < attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		attemptCtx := ctx
		var cancel context.CancelFunc
		if s.timeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, s.timeout)
		}
		object, err := s.client.GetObject(attemptCtx, s.bucket, key, minio.GetObjectOptions{})
		if err == nil {
			return s3ReadCloser{ReadCloser: object, key: key, cancel: cancel}, nil
		}
		if cancel != nil {
			cancel()
		}
		last = err
		if !retryableS3Error(ctx, err) || attempt == attempts-1 {
			break
		}
		delay := time.Duration(100*(1<<attempt)) * time.Millisecond
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		}
	}
	return nil, wrapS3Error("get block", key, last)
}

func (s *S3Store) HasBlock(ctx context.Context, tenant string, hash string) (bool, error) {
	key, err := s.blockKey(tenant, hash)
	if err != nil {
		return false, err
	}
	err = s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
		return err
	})
	if err == nil {
		return true, nil
	}
	if isS3NotFound(err) {
		return false, nil
	}
	return false, wrapS3Error("stat block", key, err)
}

func (s *S3Store) DeleteBlocks(ctx context.Context, tenant string, hashes []string) error {
	if len(hashes) == 0 {
		return nil
	}
	keys := make([]string, 0, len(hashes))
	for _, hash := range hashes {
		key, err := s.blockKey(tenant, hash)
		if err != nil {
			return err
		}
		keys = append(keys, key)
	}

	err := s.withRetry(ctx, func(ctx context.Context) error {
		objects := make(chan minio.ObjectInfo)
		go func() {
			defer close(objects)
			for _, key := range keys {
				select {
				case objects <- minio.ObjectInfo{Key: key}:
				case <-ctx.Done():
					return
				}
			}
		}()
		for deleteErr := range s.client.RemoveObjects(ctx, s.bucket, objects, minio.RemoveObjectsOptions{}) {
			if deleteErr.Err != nil && !isS3NotFound(deleteErr.Err) {
				return deleteErr.Err
			}
		}
		return ctx.Err()
	})
	return wrapS3Error("delete blocks", tenant, err)
}

func (s *S3Store) HealthCheck(ctx context.Context) error {
	err := s.withRetry(ctx, func(ctx context.Context) error {
		exists, err := s.client.BucketExists(ctx, s.bucket)
		if err != nil {
			return err
		}
		if !exists {
			return minio.ErrorResponse{Code: "NoSuchBucket", BucketName: s.bucket}
		}
		return nil
	})
	return wrapS3Error("check bucket", s.bucket, err)
}

func (s *S3Store) blockKey(tenant string, hash string) (string, error) {
	if len(hash) < 2 || strings.Contains(hash, "/") || strings.Contains(hash, "\\") || strings.Contains(hash, "..") {
		return "", fmt.Errorf("invalid block hash %q", hash)
	}
	return s.prefix + "tenants/" + sanitizePathPart(tenant) + "/blocks/" + hash[:2] + "/" + hash, nil
}

func (s *S3Store) withRetry(ctx context.Context, operation func(context.Context) error) error {
	attempts := s.maxRetries + 1
	if attempts < 1 {
		attempts = 1
	}
	var last error
	for attempt := 0; attempt < attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		attemptCtx := ctx
		var cancel context.CancelFunc
		if s.timeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, s.timeout)
		}
		last = operation(attemptCtx)
		if cancel != nil {
			cancel()
		}
		if last == nil || !retryableS3Error(ctx, last) || attempt == attempts-1 {
			return last
		}
		delay := time.Duration(100*(1<<attempt)) * time.Millisecond
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
	return last
}

type s3ReadCloser struct {
	io.ReadCloser
	key    string
	cancel context.CancelFunc
}

func (r s3ReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		return n, wrapS3Error("read block", r.key, err)
	}
	return n, err
}

func (r s3ReadCloser) Close() error {
	err := r.ReadCloser.Close()
	if r.cancel != nil {
		r.cancel()
	}
	if err != nil {
		return wrapS3Error("close block", r.key, err)
	}
	return nil
}

func normalizeEndpoint(endpoint string, secure bool) (string, bool, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return "", secure, fmt.Errorf("s3 endpoint must not be empty")
	}
	if strings.Contains(endpoint, "://") {
		parsed, err := url.Parse(endpoint)
		if err != nil {
			return "", secure, fmt.Errorf("parse s3 endpoint: %w", err)
		}
		switch parsed.Scheme {
		case "http":
			secure = false
		case "https":
			secure = true
		default:
			return "", secure, fmt.Errorf("s3 endpoint scheme must be http or https")
		}
		if parsed.Path != "" && parsed.Path != "/" {
			return "", secure, fmt.Errorf("s3 endpoint must not include a path")
		}
		endpoint = parsed.Host
	}
	endpoint = strings.TrimSuffix(endpoint, "/")
	if endpoint == "" {
		return "", secure, fmt.Errorf("s3 endpoint must not be empty")
	}
	return endpoint, secure, nil
}

func cleanS3Prefix(prefix string) string {
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	if prefix == "" {
		return ""
	}
	return prefix + "/"
}

func wrapS3Error(operation string, key string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return err
	}
	if isS3NotFound(err) {
		return fmt.Errorf("%w: %s %s", ErrBlockNotFound, operation, key)
	}
	if errors.Is(err, context.DeadlineExceeded) || isS3Unavailable(err) || minio.ToErrorResponse(err).Code == "NoSuchBucket" {
		return fmt.Errorf("%w: %s %s: %w", ErrBackendUnavailable, operation, key, err)
	}
	return fmt.Errorf("%s %s: %w", operation, key, err)
}

func isS3NotFound(err error) bool {
	response := minio.ToErrorResponse(err)
	return response.Code == "NoSuchKey" || response.Code == "NoSuchObject" || response.StatusCode == http.StatusNotFound && response.Code != "NoSuchBucket"
}

func isS3Unavailable(err error) bool {
	response := minio.ToErrorResponse(err)
	if response.StatusCode >= 500 || response.StatusCode == http.StatusTooManyRequests || response.StatusCode == http.StatusRequestTimeout {
		return true
	}
	switch response.Code {
	case "InternalError", "RequestTimeout", "ServiceUnavailable", "SlowDown", "Throttling", "ThrottlingException":
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary())
}

func retryableS3Error(parent context.Context, err error) bool {
	if parent.Err() != nil || errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || isS3Unavailable(err) {
		return true
	}
	response := minio.ToErrorResponse(err)
	if response.StatusCode == 0 && response.Code == "" {
		return true
	}
	return false
}
