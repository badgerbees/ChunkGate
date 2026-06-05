package factory

import (
	"context"
	"errors"

	"github.com/chunkgate/chunkgate/internal/backend"
	"github.com/chunkgate/chunkgate/internal/config"
)

func New(cfg config.Config) (backend.BlockStore, error) {
	if err := cfg.ValidateBackend(); err != nil {
		return nil, err
	}
	switch cfg.BackendProvider {
	case "filesystem":
		if cfg.LocalBlockEncryptionKey != "" {
			key, err := config.DecodeLocalBlockEncryptionKey(cfg.LocalBlockEncryptionKey)
			if err != nil {
				return nil, err
			}
			return backend.NewEncryptedFileStore(cfg.BackendDir, key)
		}
		return backend.NewFileStore(cfg.BackendDir), nil
	case "s3":
		return backend.NewS3Store(backend.S3Options{
			Endpoint:     cfg.S3Endpoint,
			Provider:     cfg.S3Provider,
			Region:       cfg.S3Region,
			Bucket:       cfg.S3Bucket,
			AccessKey:    cfg.S3AccessKey,
			SecretKey:    cfg.S3SecretKey,
			SessionToken: cfg.S3SessionToken,
			Prefix:       cfg.S3Prefix,
			Secure:       cfg.S3UseTLS,
			PathStyle:    cfg.S3PathStyle,
			Timeout:      cfg.S3Timeout,
			MaxRetries:   cfg.S3MaxRetries,
		})
	case "azure":
		return backend.NewAzureBlockStore(backend.AzureOptions{
			AccountName: cfg.AzureAccountName,
			AccountKey:  cfg.AzureAccountKey,
			Endpoint:    cfg.AzureEndpoint,
			Container:   cfg.AzureContainer,
			Prefix:      cfg.AzurePrefix,
			Auth:        cfg.AzureAuth,
			Timeout:     cfg.AzureTimeout,
			MaxRetries:  cfg.AzureMaxRetries,
		})
	case "gcs":
		return backend.NewGCSBlockStore(context.Background(), backend.GCSOptions{
			ProjectID:       cfg.GCSProjectID,
			Bucket:          cfg.GCSBucket,
			Endpoint:        cfg.GCSEndpoint,
			Prefix:          cfg.GCSPrefix,
			CredentialsFile: cfg.GCSCredentialsFile,
			CredentialsJSON: cfg.GCSCredentialsJSON,
			Auth:            cfg.GCSAuth,
			Timeout:         cfg.GCSTimeout,
			MaxRetries:      cfg.GCSMaxRetries,
		})
	case "swift":
		return backend.NewSwiftBlockStore(context.Background(), backend.SwiftOptions{
			AuthURL:                     cfg.SwiftAuthURL,
			Username:                    cfg.SwiftUsername,
			UserID:                      cfg.SwiftUserID,
			Password:                    cfg.SwiftPassword,
			ApplicationCredentialID:     cfg.SwiftApplicationCredID,
			ApplicationCredentialName:   cfg.SwiftApplicationCredName,
			ApplicationCredentialSecret: cfg.SwiftApplicationCredSecret,
			ProjectID:                   cfg.SwiftProjectID,
			ProjectName:                 cfg.SwiftProjectName,
			DomainID:                    cfg.SwiftDomainID,
			DomainName:                  cfg.SwiftDomainName,
			Region:                      cfg.SwiftRegion,
			Container:                   cfg.SwiftContainer,
			Endpoint:                    cfg.SwiftEndpoint,
			Prefix:                      cfg.SwiftPrefix,
			Auth:                        cfg.SwiftAuth,
			InsecureSkipVerify:          cfg.SwiftInsecureSkipVerify,
			Timeout:                     cfg.SwiftTimeout,
			MaxRetries:                  cfg.SwiftMaxRetries,
		})
	default:
		return nil, errors.New("unsupported backend provider")
	}
}
