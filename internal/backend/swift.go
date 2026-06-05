package backend

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gophercloud/gophercloud/v2"
	"github.com/gophercloud/gophercloud/v2/openstack"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/containers"
	"github.com/gophercloud/gophercloud/v2/openstack/objectstorage/v1/objects"
)

const (
	defaultSwiftTimeout         = 30 * time.Second
	defaultSwiftBulkDeleteSize  = 1000
	defaultSwiftFallbackDeletes = 8
	swiftAuthAuto               = "auto"
	swiftAuthPassword           = "password"
	swiftAuthApplicationCred    = "application-credential"
)

type SwiftOptions struct {
	AuthURL                     string
	Username                    string
	UserID                      string
	Password                    string
	ApplicationCredentialID     string
	ApplicationCredentialName   string
	ApplicationCredentialSecret string
	ProjectID                   string
	ProjectName                 string
	DomainID                    string
	DomainName                  string
	Region                      string
	Container                   string
	Endpoint                    string
	Prefix                      string
	Auth                        string
	InsecureSkipVerify          bool
	Timeout                     time.Duration
	MaxRetries                  int
}

type SwiftBlockStore struct {
	client         *gophercloud.ServiceClient
	container      string
	prefix         string
	timeout        time.Duration
	maxRetries     int
	bulkDeleteSize int
	deleteJobs     int
}

func NewSwiftBlockStore(ctx context.Context, options SwiftOptions) (*SwiftBlockStore, error) {
	if strings.TrimSpace(options.Container) == "" {
		return nil, fmt.Errorf("swift container must not be empty")
	}
	if strings.TrimSpace(options.AuthURL) == "" {
		return nil, fmt.Errorf("swift auth url must not be empty")
	}
	if options.Timeout < 0 {
		return nil, fmt.Errorf("swift timeout must be >= 0")
	}
	if options.MaxRetries < 0 {
		return nil, fmt.Errorf("swift max retries must be >= 0")
	}
	timeout := options.Timeout
	if timeout == 0 {
		timeout = defaultSwiftTimeout
	}
	auth := normalizeSwiftAuth(options.Auth)
	if auth == swiftAuthAuto {
		if options.ApplicationCredentialID != "" || options.ApplicationCredentialName != "" || options.ApplicationCredentialSecret != "" {
			auth = swiftAuthApplicationCred
		} else {
			auth = swiftAuthPassword
		}
	}
	authOptions, err := swiftAuthOptions(options, auth)
	if err != nil {
		return nil, err
	}

	provider, err := openstack.NewClient(strings.TrimSpace(options.AuthURL))
	if err != nil {
		return nil, fmt.Errorf("create swift provider client: %w", err)
	}
	provider.HTTPClient = http.Client{Transport: swiftTransport(options.InsecureSkipVerify)}
	if err := openstack.Authenticate(ctx, provider, authOptions); err != nil {
		return nil, fmt.Errorf("authenticate swift client: %w", err)
	}
	client, err := openstack.NewObjectStorageV1(provider, gophercloud.EndpointOpts{
		Region:       strings.TrimSpace(options.Region),
		Availability: gophercloud.AvailabilityPublic,
	})
	if err != nil {
		return nil, fmt.Errorf("create swift object storage client: %w", err)
	}
	if strings.TrimSpace(options.Endpoint) != "" {
		endpoint, err := canonicalSwiftEndpoint(options.Endpoint)
		if err != nil {
			return nil, err
		}
		client.Endpoint = endpoint
		client.ResourceBase = endpoint
	}

	return &SwiftBlockStore{
		client:         client,
		container:      strings.TrimSpace(options.Container),
		prefix:         cleanS3Prefix(options.Prefix),
		timeout:        timeout,
		maxRetries:     options.MaxRetries,
		bulkDeleteSize: defaultSwiftBulkDeleteSize,
		deleteJobs:     defaultSwiftFallbackDeletes,
	}, nil
}

func (s *SwiftBlockStore) PutBlock(ctx context.Context, tenant string, hash string, data []byte) error {
	key, err := s.blockKey(tenant, hash)
	if err != nil {
		return err
	}
	err = s.withRetry(ctx, func(ctx context.Context) error {
		result := objects.Create(ctx, s.client, s.container, key, objects.CreateOpts{
			Content:       bytes.NewReader(data),
			ContentLength: int64(len(data)),
			ContentType:   "application/octet-stream",
		})
		return result.Err
	})
	return wrapSwiftError("put block", key, err)
}

func (s *SwiftBlockStore) GetBlock(ctx context.Context, tenant string, hash string) (io.ReadCloser, error) {
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
		result := objects.Download(attemptCtx, s.client, s.container, key, nil)
		if result.Err == nil {
			return swiftReadCloser{ReadCloser: result.Body, key: key, cancel: cancel}, nil
		}
		if result.Body != nil {
			_ = result.Body.Close()
		}
		if cancel != nil {
			cancel()
		}
		last = result.Err
		if !retryableSwiftError(result.Err) || attempt == attempts-1 {
			break
		}
		if err := waitRetryDelay(ctx, defaultRetryBaseDelay, attempt); err != nil {
			return nil, err
		}
	}
	return nil, wrapSwiftError("get block", key, last)
}

func (s *SwiftBlockStore) HasBlock(ctx context.Context, tenant string, hash string) (bool, error) {
	key, err := s.blockKey(tenant, hash)
	if err != nil {
		return false, err
	}
	err = s.withRetry(ctx, func(ctx context.Context) error {
		return objects.Get(ctx, s.client, s.container, key, nil).Err
	})
	if err == nil {
		return true, nil
	}
	if isSwiftNotFound(err) {
		return false, nil
	}
	return false, wrapSwiftError("stat block", key, err)
}

func (s *SwiftBlockStore) DeleteBlocks(ctx context.Context, tenant string, hashes []string) error {
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
	limit := s.bulkDeleteSize
	if limit <= 0 {
		limit = defaultSwiftBulkDeleteSize
	}
	for start := 0; start < len(keys); start += limit {
		end := start + limit
		if end > len(keys) {
			end = len(keys)
		}
		batch := keys[start:end]
		var err error
		if len(batch) == 1 {
			err = s.deleteObject(ctx, batch[0])
		} else {
			err = s.deleteBatch(ctx, batch)
			if err != nil {
				err = s.deleteIndividually(ctx, batch)
			}
		}
		if err != nil {
			return wrapSwiftError("delete blocks", tenant, err)
		}
	}
	return nil
}

func (s *SwiftBlockStore) HealthCheck(ctx context.Context) error {
	err := s.withRetry(ctx, func(ctx context.Context) error {
		return containers.Get(ctx, s.client, s.container, nil).Err
	})
	if isSwiftNotFound(err) {
		return fmt.Errorf("%w: check container %s: %w", ErrBackendUnavailable, s.container, err)
	}
	return wrapSwiftError("check container", s.container, err)
}

func (s *SwiftBlockStore) blockKey(tenant string, hash string) (string, error) {
	if !validBlockHash(hash) {
		return "", fmt.Errorf("invalid block hash %q", hash)
	}
	return s.prefix + "tenants/" + sanitizePathPart(tenant) + "/blocks/" + hash[:2] + "/" + hash, nil
}

func (s *SwiftBlockStore) deleteBatch(ctx context.Context, keys []string) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		response, err := objects.BulkDelete(ctx, s.client, s.container, keys).Extract()
		if err != nil {
			return err
		}
		if len(response.Errors) > 0 {
			return fmt.Errorf("swift bulk delete reported %d errors", len(response.Errors))
		}
		return nil
	})
}

func (s *SwiftBlockStore) deleteObject(ctx context.Context, key string) error {
	err := s.withRetry(ctx, func(ctx context.Context) error {
		return objects.Delete(ctx, s.client, s.container, key, nil).Err
	})
	if isSwiftNotFound(err) {
		return nil
	}
	return err
}

func (s *SwiftBlockStore) deleteIndividually(ctx context.Context, keys []string) error {
	workers := s.deleteJobs
	if workers <= 0 {
		workers = defaultSwiftFallbackDeletes
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

func (s *SwiftBlockStore) withRetry(ctx context.Context, operation func(context.Context) error) error {
	return DoWithRetry(ctx, RetryOptions{
		Timeout:    s.timeout,
		MaxRetries: s.maxRetries,
	}, retryableSwiftError, operation)
}

type swiftReadCloser struct {
	io.ReadCloser
	key    string
	cancel context.CancelFunc
}

func (r swiftReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		return n, wrapSwiftError("read block", r.key, err)
	}
	return n, err
}

func (r swiftReadCloser) Close() error {
	err := r.ReadCloser.Close()
	if r.cancel != nil {
		r.cancel()
	}
	if err != nil {
		return wrapSwiftError("close block", r.key, err)
	}
	return nil
}

func swiftAuthOptions(options SwiftOptions, auth string) (gophercloud.AuthOptions, error) {
	authOptions := gophercloud.AuthOptions{
		IdentityEndpoint: strings.TrimSpace(options.AuthURL),
		Username:         strings.TrimSpace(options.Username),
		UserID:           strings.TrimSpace(options.UserID),
		Password:         options.Password,
		TenantID:         strings.TrimSpace(options.ProjectID),
		TenantName:       strings.TrimSpace(options.ProjectName),
		DomainID:         strings.TrimSpace(options.DomainID),
		DomainName:       strings.TrimSpace(options.DomainName),
		AllowReauth:      true,
	}
	switch auth {
	case swiftAuthPassword:
		if authOptions.Username == "" && authOptions.UserID == "" {
			return gophercloud.AuthOptions{}, fmt.Errorf("swift username or user id is required for password auth")
		}
		if authOptions.Password == "" {
			return gophercloud.AuthOptions{}, fmt.Errorf("swift password is required for password auth")
		}
	case swiftAuthApplicationCred:
		authOptions.ApplicationCredentialID = strings.TrimSpace(options.ApplicationCredentialID)
		authOptions.ApplicationCredentialName = strings.TrimSpace(options.ApplicationCredentialName)
		authOptions.ApplicationCredentialSecret = options.ApplicationCredentialSecret
		if authOptions.ApplicationCredentialID == "" && authOptions.ApplicationCredentialName == "" {
			return gophercloud.AuthOptions{}, fmt.Errorf("swift application credential id or name is required")
		}
		if authOptions.ApplicationCredentialSecret == "" {
			return gophercloud.AuthOptions{}, fmt.Errorf("swift application credential secret is required")
		}
	case swiftAuthAuto:
		return swiftAuthOptions(options, swiftAuthPassword)
	default:
		return gophercloud.AuthOptions{}, fmt.Errorf("swift auth must be auto, password, or application-credential")
	}
	return authOptions, nil
}

func canonicalSwiftEndpoint(endpoint string) (string, error) {
	endpoint = strings.TrimSpace(endpoint)
	parsed, err := url.ParseRequestURI(endpoint)
	if err != nil {
		return "", fmt.Errorf("parse swift endpoint: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("swift endpoint scheme must be http or https")
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("swift endpoint host must not be empty")
	}
	if !strings.HasSuffix(parsed.Path, "/") {
		parsed.Path += "/"
	}
	return parsed.String(), nil
}

func normalizeSwiftAuth(auth string) string {
	switch strings.ToLower(strings.TrimSpace(auth)) {
	case "", swiftAuthAuto:
		return swiftAuthAuto
	case swiftAuthPassword:
		return swiftAuthPassword
	case swiftAuthApplicationCred:
		return swiftAuthApplicationCred
	default:
		return auth
	}
}

func swiftTransport(insecureSkipVerify bool) http.RoundTripper {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if insecureSkipVerify {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	return transport
}

func wrapSwiftError(operation string, key string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return err
	}
	if isSwiftNotFound(err) {
		return fmt.Errorf("%w: %s %s", ErrBlockNotFound, operation, key)
	}
	if errors.Is(err, context.DeadlineExceeded) || isSwiftUnavailable(err) {
		return fmt.Errorf("%w: %s %s: %w", ErrBackendUnavailable, operation, key, err)
	}
	return fmt.Errorf("%s %s: %w", operation, key, err)
}

func isSwiftNotFound(err error) bool {
	return gophercloud.ResponseCodeIs(err, http.StatusNotFound)
}

func isSwiftUnavailable(err error) bool {
	if gophercloud.ResponseCodeIs(err, http.StatusRequestTimeout) ||
		gophercloud.ResponseCodeIs(err, http.StatusTooManyRequests) ||
		gophercloud.ResponseCodeIs(err, http.StatusInternalServerError) ||
		gophercloud.ResponseCodeIs(err, http.StatusBadGateway) ||
		gophercloud.ResponseCodeIs(err, http.StatusServiceUnavailable) ||
		gophercloud.ResponseCodeIs(err, http.StatusGatewayTimeout) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary())
}

func retryableSwiftError(err error) bool {
	if errors.Is(err, context.Canceled) || isSwiftNotFound(err) {
		return false
	}
	return errors.Is(err, context.DeadlineExceeded) || isSwiftUnavailable(err)
}
