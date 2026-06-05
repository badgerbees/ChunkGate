package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chunkgate/chunkgate/internal/api"
	backendfactory "github.com/chunkgate/chunkgate/internal/backend/factory"
	"github.com/chunkgate/chunkgate/internal/chunker"
	"github.com/chunkgate/chunkgate/internal/config"
	"github.com/chunkgate/chunkgate/internal/gc"
	"github.com/chunkgate/chunkgate/internal/limits"
	"github.com/chunkgate/chunkgate/internal/metadata"
	"github.com/chunkgate/chunkgate/internal/multipart"
	"github.com/chunkgate/chunkgate/internal/object"
	"github.com/chunkgate/chunkgate/internal/ops"
	"github.com/chunkgate/chunkgate/internal/s3auth"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg := config.FromEnv()
	if err := cfg.Validate(); err != nil {
		logger.Error("invalid_configuration", "error", err)
		os.Exit(1)
	}
	appCtx, stopBackground := context.WithCancel(context.Background())
	defer stopBackground()

	metadataCtx, metadataCancel := context.WithTimeout(context.Background(), 30*time.Second)
	store, err := newMetadataStore(metadataCtx, cfg)
	metadataCancel()
	if err != nil {
		logger.Error("open_metadata_store_failed", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	blocks, err := backendfactory.New(cfg)
	if err != nil {
		logger.Error("configure_backend_failed", "error", err)
		os.Exit(1)
	}
	splitter := chunker.New(chunker.Options{
		MinSize:            cfg.ChunkMinBytes,
		AvgSize:            cfg.ChunkAvgBytes,
		MaxSize:            cfg.ChunkMaxBytes,
		SmallFileThreshold: cfg.SmallFileThresholdBytes,
		Engine:             cfg.ChunkEngine,
	})

	metrics := ops.NewMetrics()
	chunkLimiter := limits.NewAdaptiveCPUSemaphore(cfg.MaxConcurrentChunkers, cfg.CPUHeadroomCores)

	objects := object.NewService(object.Config{
		Chunker: splitter,
		Backend: blocks,
		Store:   store,
		CPU:     chunkLimiter,
		Metrics: metrics,
	})

	reservations := limits.NewDiskReservations(cfg.LocalCapacityBytes)
	diskGuard := limits.NewDiskGuard(cfg.ScratchDir, reservations, cfg.ScratchMinFreeBytes)
	multipartManager := multipart.NewManager(
		cfg.ScratchDir,
		reservations,
		multipart.WithMetadataStore(store),
		multipart.WithMaxPartSize(cfg.MultipartMaxPartBytes),
		multipart.WithMaxUploadSize(cfg.MultipartMaxUploadBytes),
		multipart.WithDiskGuard(diskGuard),
	)
	startupCtx, startupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	cleanedMultipart, err := multipartManager.CleanupStale(startupCtx, cfg.MultipartStaleTTL)
	startupCancel()
	if err != nil {
		logger.Error("clean_stale_multipart_failed", "error", err)
		os.Exit(1)
	}
	if cleanedMultipart > 0 {
		logger.Info("cleaned_stale_multipart_uploads", "count", cleanedMultipart)
	}
	startupCtx, startupCancel = context.WithTimeout(context.Background(), 30*time.Second)
	err = multipartManager.LoadActive(startupCtx)
	startupCancel()
	if err != nil {
		logger.Error("load_multipart_uploads_failed", "error", err)
		os.Exit(1)
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
		logger.Error("configure_auth_failed", "error", err)
		os.Exit(1)
	}

	drain := &ops.Drain{}
	apiOptions := []api.Option{
		api.WithAuthVerifier(authVerifier),
		api.WithGCMetrics(gcMetrics),
		api.WithMetrics(metrics),
		api.WithLimiter(chunkLimiter),
		api.WithDrain(drain),
		api.WithLogger(logger),
		api.WithBodyLimits(cfg.MaxObjectBytes, cfg.MultipartMaxPartBytes, cfg.CompleteXMLMaxBytes),
		api.WithReadinessTimeout(cfg.ReadinessTimeout),
		api.WithPprof(cfg.DebugPprofEnabled),
		api.WithVirtualHosts(cfg.VirtualHosts...),
		api.WithCORS(api.CORSConfig{
			AllowedOrigins:   cfg.CORSAllowedOrigins,
			AllowedMethods:   cfg.CORSAllowedMethods,
			AllowedHeaders:   cfg.CORSAllowedHeaders,
			ExposedHeaders:   cfg.CORSExposedHeaders,
			AllowCredentials: cfg.CORSAllowCredentials,
			MaxAgeSeconds:    cfg.CORSMaxAgeSeconds,
		}),
	}
	if cfg.AuthAllowAnonymous {
		apiOptions = append(apiOptions, api.WithAnonymousTenant("default"))
	}
	handler := api.NewServer(objects, multipartManager, apiOptions...)
	server := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errs := make(chan error, 1)
	go func() {
		logger.Info("chunkgate_listening", "addr", cfg.ListenAddr)
		errs <- server.ListenAndServe()
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-stop:
		logger.Info("shutdown_signal_received", "signal", sig.String())
		drain.Start()
		stopBackground()
	case err := <-errs:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server_failed", "error", err)
			os.Exit(1)
		}
		drain.Start()
		stopBackground()
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		logger.Error("http_shutdown_failed", "error", err)
		os.Exit(1)
	}
	if err := drain.Wait(ctx); err != nil {
		logger.Error("upload_drain_failed", "error", err)
		os.Exit(1)
	}
	logger.Info("chunkgate_shutdown_complete")
}

func newMetadataStore(ctx context.Context, cfg config.Config) (metadata.Store, error) {
	switch cfg.MetadataProvider {
	case "sqlite":
		return metadata.NewSQLiteStore(cfg.MetadataDir)
	case "postgres":
		return metadata.NewPostgresStore(ctx, metadata.PostgresOptions{
			DSN:             cfg.PostgresDSN,
			MaxOpenConns:    cfg.PostgresMaxOpenConns,
			MaxIdleConns:    cfg.PostgresMaxIdleConns,
			ConnMaxLifetime: cfg.PostgresConnMaxLifetime,
		})
	default:
		return nil, errors.New("unsupported metadata provider")
	}
}
