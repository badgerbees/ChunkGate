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

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

const (
	defaultGCSTimeout         = 30 * time.Second
	defaultGCSFallbackDeletes = 8
	gcsAuthAuto               = "auto"
	gcsAuthServiceAccount     = "service-account"
	gcsAuthDefault            = "default"
	gcsAuthEmulator           = "emulator"
)

type GCSOptions struct {
	ProjectID       string
	Bucket          string
	Endpoint        string
	Prefix          string
	CredentialsFile string
	CredentialsJSON string
	Auth            string
	Timeout         time.Duration
	MaxRetries      int
}

type GCSBlockStore struct {
	client     *storage.Client
	bucket     *storage.BucketHandle
	bucketName string
	prefix     string
	timeout    time.Duration
	maxRetries int
	deleteJobs int
}

func NewGCSBlockStore(ctx context.Context, options GCSOptions) (*GCSBlockStore, error) {
	if strings.TrimSpace(options.Bucket) == "" {
		return nil, fmt.Errorf("gcs bucket must not be empty")
	}
	if options.Timeout < 0 {
		return nil, fmt.Errorf("gcs timeout must be >= 0")
	}
	if options.MaxRetries < 0 {
		return nil, fmt.Errorf("gcs max retries must be >= 0")
	}
	timeout := options.Timeout
	if timeout == 0 {
		timeout = defaultGCSTimeout
	}
	clientOptions, err := gcsClientOptions(options)
	if err != nil {
		return nil, err
	}
	clientOptions = append(clientOptions, storage.WithJSONReads())
	client, err := storage.NewClient(ctx, clientOptions...)
	if err != nil {
		return nil, fmt.Errorf("create gcs client: %w", err)
	}
	bucketName := strings.TrimSpace(options.Bucket)
	return &GCSBlockStore{
		client:     client,
		bucket:     client.Bucket(bucketName),
		bucketName: bucketName,
		prefix:     cleanS3Prefix(options.Prefix),
		timeout:    timeout,
		maxRetries: options.MaxRetries,
		deleteJobs: defaultGCSFallbackDeletes,
	}, nil
}

func (s *GCSBlockStore) PutBlock(ctx context.Context, tenant string, hash string, data []byte) error {
	key, err := s.blockKey(tenant, hash)
	if err != nil {
		return err
	}
	err = s.withRetry(ctx, func(ctx context.Context) error {
		writer := s.bucket.Object(key).NewWriter(ctx)
		writer.ContentType = "application/octet-stream"
		writer.ChunkSize = 0
		if _, err := io.Copy(writer, bytes.NewReader(data)); err != nil {
			_ = writer.CloseWithError(err)
			return err
		}
		return writer.Close()
	})
	return wrapGCSError("put block", key, err)
}

func (s *GCSBlockStore) GetBlock(ctx context.Context, tenant string, hash string) (io.ReadCloser, error) {
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
		reader, err := s.bucket.Object(key).NewReader(attemptCtx)
		if err == nil {
			return gcsReadCloser{ReadCloser: reader, key: key, cancel: cancel}, nil
		}
		if cancel != nil {
			cancel()
		}
		last = err
		if !retryableGCSError(err) || attempt == attempts-1 {
			break
		}
		if err := waitRetryDelay(ctx, defaultRetryBaseDelay, attempt); err != nil {
			return nil, err
		}
	}
	return nil, wrapGCSError("get block", key, last)
}

func (s *GCSBlockStore) HasBlock(ctx context.Context, tenant string, hash string) (bool, error) {
	key, err := s.blockKey(tenant, hash)
	if err != nil {
		return false, err
	}
	err = s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.bucket.Object(key).Attrs(ctx)
		return err
	})
	if err == nil {
		return true, nil
	}
	if isGCSObjectNotFound(err) {
		return false, nil
	}
	return false, wrapGCSError("stat block", key, err)
}

func (s *GCSBlockStore) DeleteBlocks(ctx context.Context, tenant string, hashes []string) error {
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
	if err := s.deleteIndividually(ctx, keys); err != nil {
		return wrapGCSError("delete blocks", tenant, err)
	}
	return nil
}

func (s *GCSBlockStore) HealthCheck(ctx context.Context) error {
	err := s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.bucket.Attrs(ctx)
		return err
	})
	return wrapGCSError("check bucket", s.bucketName, err)
}

func (s *GCSBlockStore) blockKey(tenant string, hash string) (string, error) {
	if !validBlockHash(hash) {
		return "", fmt.Errorf("invalid block hash %q", hash)
	}
	return s.prefix + "tenants/" + sanitizePathPart(tenant) + "/blocks/" + hash[:2] + "/" + hash, nil
}

func (s *GCSBlockStore) deleteIndividually(ctx context.Context, keys []string) error {
	workers := s.deleteJobs
	if workers <= 0 {
		workers = defaultGCSFallbackDeletes
	}
	if workers > len(keys) {
		workers = len(keys)
	}
	jobs := make(chan string)
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		go func() {
			var first error
			for key := range jobs {
				if err := s.deleteObject(ctx, key); err != nil && first == nil {
					first = err
				}
			}
			errs <- first
		}()
	}
	for _, key := range keys {
		select {
		case jobs <- key:
		case <-ctx.Done():
			close(jobs)
			for i := 0; i < workers; i++ {
				<-errs
			}
			return ctx.Err()
		}
	}
	close(jobs)
	var first error
	for i := 0; i < workers; i++ {
		if err := <-errs; err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (s *GCSBlockStore) deleteObject(ctx context.Context, key string) error {
	err := s.withRetry(ctx, func(ctx context.Context) error {
		return s.bucket.Object(key).Delete(ctx)
	})
	if isGCSObjectNotFound(err) {
		return nil
	}
	return err
}

func (s *GCSBlockStore) withRetry(ctx context.Context, operation func(context.Context) error) error {
	return DoWithRetry(ctx, RetryOptions{
		Timeout:    s.timeout,
		MaxRetries: s.maxRetries,
	}, retryableGCSError, operation)
}

type gcsReadCloser struct {
	io.ReadCloser
	key    string
	cancel context.CancelFunc
}

func (r gcsReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		return n, wrapGCSError("read block", r.key, err)
	}
	return n, err
}

func (r gcsReadCloser) Close() error {
	err := r.ReadCloser.Close()
	if r.cancel != nil {
		r.cancel()
	}
	if err != nil {
		return wrapGCSError("close block", r.key, err)
	}
	return nil
}

func gcsClientOptions(options GCSOptions) ([]option.ClientOption, error) {
	auth := normalizeGCSAuth(options.Auth)
	if auth == gcsAuthAuto {
		switch {
		case options.CredentialsFile != "" || options.CredentialsJSON != "":
			auth = gcsAuthServiceAccount
		case options.Endpoint != "":
			auth = gcsAuthEmulator
		default:
			auth = gcsAuthDefault
		}
	}
	clientOptions := make([]option.ClientOption, 0, 3)
	if options.Endpoint != "" {
		endpoint, err := canonicalGCSEndpoint(options.Endpoint, auth)
		if err != nil {
			return nil, fmt.Errorf("parse gcs endpoint: %w", err)
		}
		clientOptions = append(clientOptions, option.WithEndpoint(endpoint))
	}
	switch auth {
	case gcsAuthServiceAccount:
		if options.CredentialsFile != "" && options.CredentialsJSON != "" {
			return nil, fmt.Errorf("gcs credentials file and json are mutually exclusive")
		}
		if options.CredentialsFile != "" {
			clientOptions = append(clientOptions, option.WithAuthCredentialsFile(option.ServiceAccount, options.CredentialsFile))
		} else if options.CredentialsJSON != "" {
			clientOptions = append(clientOptions, option.WithAuthCredentialsJSON(option.ServiceAccount, []byte(options.CredentialsJSON)))
		} else {
			return nil, fmt.Errorf("gcs service-account auth requires credentials file or json")
		}
	case gcsAuthDefault:
	case gcsAuthEmulator:
		clientOptions = append(clientOptions, option.WithoutAuthentication())
	default:
		return nil, fmt.Errorf("gcs auth must be auto, service-account, default, or emulator")
	}
	return clientOptions, nil
}

func canonicalGCSEndpoint(endpoint string, auth string) (string, error) {
	endpoint = strings.TrimSpace(endpoint)
	parsed, err := url.ParseRequestURI(endpoint)
	if err != nil {
		return "", err
	}
	if auth == gcsAuthEmulator && parsed.Path == "" {
		parsed.Path = "/storage/v1/"
		return parsed.String(), nil
	}
	if parsed.Path != "" && !strings.HasSuffix(parsed.Path, "/") {
		parsed.Path += "/"
		return parsed.String(), nil
	}
	return endpoint, nil
}

func normalizeGCSAuth(auth string) string {
	switch strings.ToLower(strings.TrimSpace(auth)) {
	case "", gcsAuthAuto:
		return gcsAuthAuto
	case gcsAuthServiceAccount:
		return gcsAuthServiceAccount
	case gcsAuthDefault:
		return gcsAuthDefault
	case gcsAuthEmulator:
		return gcsAuthEmulator
	default:
		return auth
	}
}

func wrapGCSError(operation string, key string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return err
	}
	if isGCSObjectNotFound(err) {
		return fmt.Errorf("%w: %s %s", ErrBlockNotFound, operation, key)
	}
	if errors.Is(err, context.DeadlineExceeded) || isGCSBucketNotFound(err) || isGCSUnavailable(err) {
		return fmt.Errorf("%w: %s %s: %w", ErrBackendUnavailable, operation, key, err)
	}
	return fmt.Errorf("%s %s: %w", operation, key, err)
}

func isGCSObjectNotFound(err error) bool {
	return errors.Is(err, storage.ErrObjectNotExist)
}

func isGCSBucketNotFound(err error) bool {
	return errors.Is(err, storage.ErrBucketNotExist)
}

func isGCSUnavailable(err error) bool {
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		return apiErr.Code >= 500 || apiErr.Code == http.StatusTooManyRequests || apiErr.Code == http.StatusRequestTimeout
	}
	var netErr net.Error
	return errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary())
}

func retryableGCSError(err error) bool {
	if errors.Is(err, context.Canceled) || isGCSObjectNotFound(err) {
		return false
	}
	return errors.Is(err, context.DeadlineExceeded) || isGCSUnavailable(err)
}
