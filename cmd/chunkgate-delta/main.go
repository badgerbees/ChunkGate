package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/chunkgate/chunkgate/internal/deltaclient"
)

func main() {
	if len(os.Args) < 2 || os.Args[1] != "get" {
		usage()
		os.Exit(2)
	}
	if err := runGet(os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runGet(args []string) error {
	flags := flag.NewFlagSet("get", flag.ExitOnError)
	endpoint := flags.String("endpoint", envOr("CHUNKGATE_ENDPOINT", "http://localhost:8080"), "ChunkGate endpoint URL")
	bucket := flags.String("bucket", "", "S3 bucket name")
	key := flags.String("key", "", "S3 object key")
	output := flags.String("output", "", "output file path")
	cacheDir := flags.String("cache-dir", envOr("CHUNKGATE_DELTA_CACHE_DIR", ".chunkgate-cache"), "local block cache directory")
	accessKey := flags.String("access-key", firstEnv("CHUNKGATE_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID"), "ChunkGate access key")
	secretKey := flags.String("secret-key", firstEnv("CHUNKGATE_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY"), "ChunkGate secret key")
	region := flags.String("region", envOr("AWS_REGION", "us-east-1"), "SigV4 region")
	timeout := flags.Duration("timeout", 5*time.Minute, "request timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *bucket == "" || *key == "" || *output == "" {
		return fmt.Errorf("get requires -bucket, -key, and -output")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	client := deltaclient.Client{
		Endpoint:  *endpoint,
		AccessKey: *accessKey,
		SecretKey: *secretKey,
		Region:    *region,
	}
	result, err := client.Download(ctx, *bucket, *key, *output, *cacheDir)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "downloaded %s/%s to %s (%d bytes, %d chunks, %d fetched)\n", *bucket, *key, *output, result.Bytes, result.TotalChunks, result.MissingChunks)
	return nil
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: chunkgate-delta get -endpoint http://localhost:8080 -bucket BUCKET -key KEY -output FILE [-cache-dir DIR]")
}

func envOr(key string, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func firstEnv(keys ...string) string {
	for _, key := range keys {
		if value := os.Getenv(key); value != "" {
			return value
		}
	}
	return ""
}
