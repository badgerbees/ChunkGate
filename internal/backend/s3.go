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
	client     s3ObjectClient
	bucket     string
	prefix     string
	timeout    time.Duration
	maxRetries int
}

type s3ObjectClient interface {
	PutObject(ctx context.Context, bucket string, key string, data []byte) error
	GetObject(ctx context.Context, bucket string, key string) (io.ReadCloser, error)
	StatObject(ctx context.Context, bucket string, key string) error
	DeleteObjects(ctx context.Context, bucket string, keys []string) error
	BucketExists(ctx context.Context, bucket string) (bool, error)
}

func NewS3Store(options S3Options) (*S3Store, error) {
	endpoint, err := normalizeEndpoint(options.Endpoint, options.Secure)
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
	var client s3ObjectClient
	if endpoint.BasePath != "" {
		client = newPathS3Client(pathS3Options{
			Endpoint:     endpoint,
			Region:       options.Region,
			AccessKey:    options.AccessKey,
			SecretKey:    options.SecretKey,
			SessionToken: options.SessionToken,
			PathStyle:    options.PathStyle,
		})
	} else {
		minioClient, err := minio.New(endpoint.Host, &minio.Options{
			Creds:        credentials.NewStaticV4(options.AccessKey, options.SecretKey, options.SessionToken),
			Secure:       endpoint.Secure,
			Region:       options.Region,
			BucketLookup: bucketLookup,
			MaxRetries:   1,
		})
		if err != nil {
			return nil, fmt.Errorf("create s3 client: %w", err)
		}
		client = minioS3Client{client: minioClient}
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
		return s.client.PutObject(ctx, s.bucket, key, data)
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
		object, err := s.client.GetObject(attemptCtx, s.bucket, key)
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
		return s.client.StatObject(ctx, s.bucket, key)
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
		return s.client.DeleteObjects(ctx, s.bucket, keys)
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
	if !validBlockHash(hash) {
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

type minioS3Client struct {
	client *minio.Client
}

func (c minioS3Client) PutObject(ctx context.Context, bucket string, key string, data []byte) error {
	_, err := c.client.PutObject(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	return err
}

func (c minioS3Client) GetObject(ctx context.Context, bucket string, key string) (io.ReadCloser, error) {
	return c.client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
}

func (c minioS3Client) StatObject(ctx context.Context, bucket string, key string) error {
	_, err := c.client.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
	return err
}

func (c minioS3Client) DeleteObjects(ctx context.Context, bucket string, keys []string) error {
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
	for deleteErr := range c.client.RemoveObjects(ctx, bucket, objects, minio.RemoveObjectsOptions{}) {
		if deleteErr.Err != nil && !isS3NotFound(deleteErr.Err) {
			return deleteErr.Err
		}
	}
	return ctx.Err()
}

func (c minioS3Client) BucketExists(ctx context.Context, bucket string) (bool, error) {
	return c.client.BucketExists(ctx, bucket)
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

type normalizedS3Endpoint struct {
	Host     string
	BasePath string
	Secure   bool
}

func normalizeEndpoint(endpoint string, secure bool) (normalizedS3Endpoint, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return normalizedS3Endpoint{}, fmt.Errorf("s3 endpoint must not be empty")
	}
	normalized := normalizedS3Endpoint{Secure: secure}
	if strings.Contains(endpoint, "://") {
		parsed, err := url.Parse(endpoint)
		if err != nil {
			return normalizedS3Endpoint{}, fmt.Errorf("parse s3 endpoint: %w", err)
		}
		switch parsed.Scheme {
		case "http":
			normalized.Secure = false
		case "https":
			normalized.Secure = true
		default:
			return normalizedS3Endpoint{}, fmt.Errorf("s3 endpoint scheme must be http or https")
		}
		normalized.BasePath = cleanEndpointBasePath(parsed.Path)
		endpoint = parsed.Host
	}
	endpoint = strings.TrimSuffix(endpoint, "/")
	if endpoint == "" {
		return normalizedS3Endpoint{}, fmt.Errorf("s3 endpoint must not be empty")
	}
	normalized.Host = endpoint
	return normalized, nil
}

func cleanEndpointBasePath(basePath string) string {
	basePath = strings.TrimSpace(basePath)
	if basePath == "" || basePath == "/" {
		return ""
	}
	return "/" + strings.Trim(basePath, "/")
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
	if errors.Is(err, context.DeadlineExceeded) || isS3Unavailable(err) || isS3NoSuchBucket(err) {
		return fmt.Errorf("%w: %s %s: %w", ErrBackendUnavailable, operation, key, err)
	}
	return fmt.Errorf("%s %s: %w", operation, key, err)
}

func isS3NotFound(err error) bool {
	var httpErr s3HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.Code == "NoSuchKey" || httpErr.Code == "NoSuchObject" || httpErr.StatusCode == http.StatusNotFound && httpErr.Code != "NoSuchBucket"
	}
	response := minio.ToErrorResponse(err)
	return response.Code == "NoSuchKey" || response.Code == "NoSuchObject" || response.StatusCode == http.StatusNotFound && response.Code != "NoSuchBucket"
}

func isS3NoSuchBucket(err error) bool {
	var httpErr s3HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.Code == "NoSuchBucket"
	}
	return minio.ToErrorResponse(err).Code == "NoSuchBucket"
}

func isS3Unavailable(err error) bool {
	var httpErr s3HTTPError
	if errors.As(err, &httpErr) {
		if httpErr.StatusCode >= 500 || httpErr.StatusCode == http.StatusTooManyRequests || httpErr.StatusCode == http.StatusRequestTimeout {
			return true
		}
		switch httpErr.Code {
		case "InternalError", "RequestTimeout", "ServiceUnavailable", "SlowDown", "Throttling", "ThrottlingException":
			return true
		}
	}
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
