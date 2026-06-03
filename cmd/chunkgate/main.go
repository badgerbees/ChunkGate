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
	"github.com/chunkgate/chunkgate/internal/limits"
	"github.com/chunkgate/chunkgate/internal/metadata"
	"github.com/chunkgate/chunkgate/internal/multipart"
	"github.com/chunkgate/chunkgate/internal/object"
)

func main() {
	cfg := config.FromEnv()
	if err := cfg.Validate(); err != nil {
		log.Fatalf("invalid configuration: %v", err)
	}

	store, err := metadata.NewSQLiteStore(cfg.MetadataDir)
	if err != nil {
		log.Fatalf("open metadata store: %v", err)
	}
	defer store.Close()

	blocks := backend.NewFileStore(cfg.BackendDir)
	splitter := chunker.New(chunker.Options{
		MinSize:            cfg.ChunkMinBytes,
		AvgSize:            cfg.ChunkAvgBytes,
		MaxSize:            cfg.ChunkMaxBytes,
		SmallFileThreshold: cfg.SmallFileThresholdBytes,
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
	)

	server := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           api.NewServer(objects, multipartManager),
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
	case err := <-errs:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server failed: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("shutdown failed: %v", err)
	}
}
