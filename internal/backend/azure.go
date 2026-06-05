package backend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

const (
	defaultAzureTimeout          = 30 * time.Second
	defaultAzureDeleteBatchSize  = 256
	defaultAzureFallbackDeletes  = 8
	azureAuthAuto                = "auto"
	azureAuthSharedKey           = "shared-key"
	azureAuthDefault             = "default"
	azureErrorBlobNotFound       = "BlobNotFound"
	azureErrorContainerNotFound  = "ContainerNotFound"
	azureErrorResourceNotFound   = "ResourceNotFound"
	azureErrorAuthenticationFail = "AuthenticationFailed"
)

type AzureOptions struct {
	AccountName string
	AccountKey  string
	Endpoint    string
	Container   string
	Prefix      string
	Auth        string
	Timeout     time.Duration
	MaxRetries  int
}

type AzureBlockStore struct {
	blobClient      *azblob.Client
	containerClient *container.Client
	container       string
	prefix          string
	timeout         time.Duration
	maxRetries      int
	deleteBatchSize int
}

func NewAzureBlockStore(options AzureOptions) (*AzureBlockStore, error) {
	if strings.TrimSpace(options.Container) == "" {
		return nil, fmt.Errorf("azure container must not be empty")
	}
	if options.Timeout < 0 {
		return nil, fmt.Errorf("azure timeout must be >= 0")
	}
	if options.MaxRetries < 0 {
		return nil, fmt.Errorf("azure max retries must be >= 0")
	}
	timeout := options.Timeout
	if timeout == 0 {
		timeout = defaultAzureTimeout
	}

	serviceURL, err := azureServiceURL(options.Endpoint, options.AccountName)
	if err != nil {
		return nil, err
	}
	containerURL, err := azureContainerURL(serviceURL, options.Container)
	if err != nil {
		return nil, err
	}

	auth := normalizeAzureAuth(options.Auth)
	if auth == azureAuthAuto {
		if options.AccountKey != "" {
			auth = azureAuthSharedKey
		} else {
			auth = azureAuthDefault
		}
	}

	var blobClient *azblob.Client
	var containerClient *container.Client
	switch auth {
	case azureAuthSharedKey:
		if options.AccountName == "" || options.AccountKey == "" {
			return nil, fmt.Errorf("azure account name and account key are required for shared-key auth")
		}
		cred, err := azblob.NewSharedKeyCredential(options.AccountName, options.AccountKey)
		if err != nil {
			return nil, fmt.Errorf("configure azure shared key: %w", err)
		}
		blobClient, err = azblob.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("create azure blob client: %w", err)
		}
		containerClient, err = container.NewClientWithSharedKeyCredential(containerURL, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("create azure container client: %w", err)
		}
	case azureAuthDefault:
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("configure default azure credential: %w", err)
		}
		blobClient, err = azblob.NewClient(serviceURL, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("create azure blob client: %w", err)
		}
		containerClient, err = container.NewClient(containerURL, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("create azure container client: %w", err)
		}
	default:
		return nil, fmt.Errorf("azure auth must be auto, shared-key, or default")
	}

	return &AzureBlockStore{
		blobClient:      blobClient,
		containerClient: containerClient,
		container:       strings.TrimSpace(options.Container),
		prefix:          cleanS3Prefix(options.Prefix),
		timeout:         timeout,
		maxRetries:      options.MaxRetries,
		deleteBatchSize: defaultAzureDeleteBatchSize,
	}, nil
}

func (s *AzureBlockStore) PutBlock(ctx context.Context, tenant string, hash string, data []byte) error {
	key, err := s.blockKey(tenant, hash)
	if err != nil {
		return err
	}
	err = s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.blobClient.UploadBuffer(ctx, s.container, key, data, nil)
		return err
	})
	return wrapAzureError("put block", key, err)
}

func (s *AzureBlockStore) GetBlock(ctx context.Context, tenant string, hash string) (io.ReadCloser, error) {
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
		response, err := s.blobClient.DownloadStream(attemptCtx, s.container, key, nil)
		if err == nil {
			reader := response.NewRetryReader(attemptCtx, nil)
			return azureReadCloser{ReadCloser: reader, key: key, cancel: cancel}, nil
		}
		if cancel != nil {
			cancel()
		}
		last = err
		if !retryableAzureError(err) || attempt == attempts-1 {
			break
		}
		if err := waitRetryDelay(ctx, defaultRetryBaseDelay, attempt); err != nil {
			return nil, err
		}
	}
	return nil, wrapAzureError("get block", key, last)
}

func (s *AzureBlockStore) HasBlock(ctx context.Context, tenant string, hash string) (bool, error) {
	key, err := s.blockKey(tenant, hash)
	if err != nil {
		return false, err
	}
	err = s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.containerClient.NewBlobClient(key).GetProperties(ctx, nil)
		return err
	})
	if err == nil {
		return true, nil
	}
	if isAzureBlobNotFound(err) {
		return false, nil
	}
	return false, wrapAzureError("stat block", key, err)
}

func (s *AzureBlockStore) DeleteBlocks(ctx context.Context, tenant string, hashes []string) error {
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
	limit := s.deleteBatchSize
	if limit <= 0 {
		limit = defaultAzureDeleteBatchSize
	}
	for start := 0; start < len(keys); start += limit {
		end := start + limit
		if end > len(keys) {
			end = len(keys)
		}
		batch := keys[start:end]
		var err error
		if len(batch) == 1 {
			err = s.deleteBlob(ctx, batch[0])
		} else {
			err = s.deleteBatch(ctx, batch)
			if err != nil {
				err = s.deleteIndividually(ctx, batch)
			}
		}
		if err != nil {
			return wrapAzureError("delete blocks", tenant, err)
		}
	}
	return nil
}

func (s *AzureBlockStore) HealthCheck(ctx context.Context) error {
	err := s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.containerClient.GetProperties(ctx, nil)
		return err
	})
	return wrapAzureError("check container", s.container, err)
}

func (s *AzureBlockStore) blockKey(tenant string, hash string) (string, error) {
	if !validBlockHash(hash) {
		return "", fmt.Errorf("invalid block hash %q", hash)
	}
	return s.prefix + "tenants/" + sanitizePathPart(tenant) + "/blocks/" + hash[:2] + "/" + hash, nil
}

func (s *AzureBlockStore) deleteBatch(ctx context.Context, keys []string) error {
	builder, err := s.containerClient.NewBatchBuilder()
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err := builder.Delete(key, nil); err != nil {
			return err
		}
	}
	return s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.containerClient.SubmitBatch(ctx, builder, nil)
		return err
	})
}

func (s *AzureBlockStore) deleteBlob(ctx context.Context, key string) error {
	err := s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.blobClient.DeleteBlob(ctx, s.container, key, nil)
		return err
	})
	if isAzureBlobNotFound(err) {
		return nil
	}
	return err
}

func (s *AzureBlockStore) deleteIndividually(ctx context.Context, keys []string) error {
	sem := make(chan struct{}, defaultAzureFallbackDeletes)
	errs := make(chan error, len(keys))
	for _, key := range keys {
		key := key
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		go func() {
			defer func() { <-sem }()
			errs <- s.deleteBlob(ctx, key)
		}()
	}
	var first error
	for range keys {
		if err := <-errs; err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (s *AzureBlockStore) withRetry(ctx context.Context, operation func(context.Context) error) error {
	return DoWithRetry(ctx, RetryOptions{
		Timeout:    s.timeout,
		MaxRetries: s.maxRetries,
	}, retryableAzureError, operation)
}

type azureReadCloser struct {
	io.ReadCloser
	key    string
	cancel context.CancelFunc
}

func (r azureReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		return n, wrapAzureError("read block", r.key, err)
	}
	return n, err
}

func (r azureReadCloser) Close() error {
	err := r.ReadCloser.Close()
	if r.cancel != nil {
		r.cancel()
	}
	if err != nil {
		return wrapAzureError("close block", r.key, err)
	}
	return nil
}

func azureServiceURL(endpoint string, accountName string) (string, error) {
	endpoint = strings.TrimRight(strings.TrimSpace(endpoint), "/")
	if endpoint != "" {
		if _, err := url.ParseRequestURI(endpoint); err != nil {
			return "", fmt.Errorf("parse azure endpoint: %w", err)
		}
		return endpoint, nil
	}
	accountName = strings.TrimSpace(accountName)
	if accountName == "" {
		return "", fmt.Errorf("azure account name or endpoint must be set")
	}
	return "https://" + accountName + ".blob.core.windows.net", nil
}

func azureContainerURL(serviceURL string, containerName string) (string, error) {
	parsed, err := url.Parse(serviceURL)
	if err != nil {
		return "", fmt.Errorf("parse azure service endpoint: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("azure endpoint scheme must be http or https")
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("azure endpoint host must not be empty")
	}
	parsed.Path = joinS3Path(parsed.Path, strings.TrimSpace(containerName))
	return parsed.String(), nil
}

func normalizeAzureAuth(auth string) string {
	switch strings.ToLower(strings.TrimSpace(auth)) {
	case "", azureAuthAuto:
		return azureAuthAuto
	case azureAuthSharedKey:
		return azureAuthSharedKey
	case azureAuthDefault:
		return azureAuthDefault
	default:
		return auth
	}
}

func wrapAzureError(operation string, key string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return err
	}
	if isAzureBlobNotFound(err) {
		return fmt.Errorf("%w: %s %s", ErrBlockNotFound, operation, key)
	}
	if errors.Is(err, context.DeadlineExceeded) || isAzureContainerNotFound(err) || isAzureUnavailable(err) {
		return fmt.Errorf("%w: %s %s: %w", ErrBackendUnavailable, operation, key, err)
	}
	return fmt.Errorf("%s %s: %w", operation, key, err)
}

func isAzureBlobNotFound(err error) bool {
	var response *azcore.ResponseError
	if errors.As(err, &response) {
		return response.ErrorCode == azureErrorBlobNotFound ||
			response.ErrorCode == azureErrorResourceNotFound ||
			response.StatusCode == http.StatusNotFound && response.ErrorCode != azureErrorContainerNotFound
	}
	return false
}

func isAzureContainerNotFound(err error) bool {
	var response *azcore.ResponseError
	return errors.As(err, &response) && response.ErrorCode == azureErrorContainerNotFound
}

func isAzureUnavailable(err error) bool {
	var response *azcore.ResponseError
	if errors.As(err, &response) {
		if response.StatusCode >= 500 || response.StatusCode == http.StatusTooManyRequests || response.StatusCode == http.StatusRequestTimeout {
			return true
		}
		switch response.ErrorCode {
		case "ServerBusy", "OperationTimedOut", "InternalError", azureErrorAuthenticationFail:
			return true
		}
	}
	var netErr net.Error
	return errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary())
}

func retryableAzureError(err error) bool {
	if errors.Is(err, context.Canceled) || isAzureBlobNotFound(err) {
		return false
	}
	return errors.Is(err, context.DeadlineExceeded) || isAzureUnavailable(err)
}
