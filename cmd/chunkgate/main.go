package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/chunkgate/chunkgate/internal/api"
	"github.com/chunkgate/chunkgate/internal/backend"
	"github.com/chunkgate/chunkgate/internal/chunker"
	"github.com/chunkgate/chunkgate/internal/config"
	"github.com/chunkgate/chunkgate/internal/gc"
	"github.com/chunkgate/chunkgate/internal/limits"
	"github.com/chunkgate/chunkgate/internal/metadata"
	"github.com/chunkgate/chunkgate/internal/multipart"
	"github.com/chunkgate/chunkgate/internal/object"
	"github.com/chunkgate/chunkgate/internal/s3auth"
)

func main() {
	cfg := config.FromEnv()
	if err := cfg.Validate(); err != nil {
		log.Fatalf("invalid configuration: %v", err)
	}
	appCtx, stopBackground := context.WithCancel(context.Background())
	defer stopBackground()

	store, err := metadata.NewSQLiteStore(cfg.MetadataDir)
	if err != nil {
		log.Fatalf("open metadata store: %v", err)
	}
	defer store.Close()

	blocks, err := newBlockStore(cfg)
	if err != nil {
		log.Fatalf("configure backend: %v", err)
	}
	splitter := chunker.New(chunker.Options{
		MinSize:            cfg.ChunkMinBytes,
		AvgSize:            cfg.ChunkAvgBytes,
		MaxSize:            cfg.ChunkMaxBytes,
		SmallFileThreshold: cfg.SmallFileThresholdBytes,
		Engine:             cfg.ChunkEngine,
	})

	maxChunkers := cfg.MaxConcurrentChunkers
	if maxChunkers <= 0 {
		maxChunkers = runtime.NumCPU()
	}

	objects := object.NewService(object.Config{
		Chunker: splitter,
		Backend: blocks,
		Store:   store,
		CPU:     limits.NewCPUSemaphore(maxChunkers),
	})

	multipartManager := multipart.NewManager(
		cfg.ScratchDir,
		limits.NewDiskReservations(cfg.LocalCapacityBytes),
		multipart.WithMetadataStore(store),
		multipart.WithMaxPartSize(cfg.MultipartMaxPartBytes),
		multipart.WithMaxUploadSize(cfg.MultipartMaxUploadBytes),
	)
	startupCtx, startupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	cleanedMultipart, err := multipartManager.CleanupStale(startupCtx, cfg.MultipartStaleTTL)
	startupCancel()
	if err != nil {
		log.Fatalf("clean stale multipart uploads: %v", err)
	}
	if cleanedMultipart > 0 {
		log.Printf("cleaned %d stale multipart uploads", cleanedMultipart)
	}
	startupCtx, startupCancel = context.WithTimeout(context.Background(), 30*time.Second)
	err = multipartManager.LoadActive(startupCtx)
	startupCancel()
	if err != nil {
		log.Fatalf("load multipart uploads: %v", err)
	}

	gcMetrics := gc.NewMetrics()
	gcSweeper := &gc.Sweeper{
		Store:        store,
		Backend:      blocks,
		BatchSize:    cfg.GCBatchSize,
		MinOrphanAge: cfg.GCMinOrphanAge,
		MaxRetries:   cfg.GCMaxRetries,
		Metrics:      gcMetrics,
	}
	if cfg.GCEnabled {
		go (gc.Worker{
			Sweeper:  gcSweeper,
			Interval: cfg.GCInterval,
			Logger:   log.Default(),
		}).Run(appCtx)
	}

	authVerifier, err := s3auth.NewVerifier(cfg.AuthCredentials)
	if err != nil {
		log.Fatalf("configure auth: %v", err)
	}

	server := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           api.NewServer(objects, multipartManager, api.WithAuthVerifier(authVerifier), api.WithGCMetrics(gcMetrics)),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errs := make(chan error, 1)
	go func() {
		log.Printf("chunkgate listening on %s", cfg.ListenAddr)
		errs <- server.ListenAndServe()
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-stop:
		log.Printf("received %s, shutting down", sig)
		stopBackground()
	case err := <-errs:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server failed: %v", err)
		}
		stopBackground()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("shutdown failed: %v", err)
	}
}

func newBlockStore(cfg config.Config) (backend.BlockStore, error) {
	switch cfg.BackendProvider {
	case "filesystem":
		return backend.NewFileStore(cfg.BackendDir), nil
	case "s3":
		return backend.NewS3Store(backend.S3Options{
			Endpoint:     cfg.S3Endpoint,
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
	default:
		return nil, errors.New("unsupported backend provider")
	}
}
