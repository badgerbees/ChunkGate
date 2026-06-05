package deltaclient

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type Client struct {
	Endpoint   string
	AccessKey  string
	SecretKey  string
	Region     string
	HTTPClient *http.Client
}

type Manifest struct {
	Version   int               `json:"version"`
	Bucket    string            `json:"bucket"`
	Key       string            `json:"key"`
	Size      int64             `json:"size"`
	ETag      string            `json:"etag"`
	ObjectMD5 string            `json:"object_md5,omitempty"`
	Headers   map[string]string `json:"headers,omitempty"`
	Chunks    []ManifestChunk   `json:"chunks"`
}

type ManifestChunk struct {
	Index  int    `json:"index"`
	Hash   string `json:"hash"`
	Offset int64  `json:"offset"`
	Size   int64  `json:"size"`
}

type DownloadResult struct {
	Bytes         int64
	TotalChunks   int
	MissingChunks int
}

type HTTPError struct {
	StatusCode int
	Body       string
}

func (e HTTPError) Error() string {
	return fmt.Sprintf("chunkgate companion request failed with status %d: %s", e.StatusCode, strings.TrimSpace(e.Body))
}

func (c Client) FetchManifest(ctx context.Context, bucket string, key string) (Manifest, error) {
	values := url.Values{}
	values.Set("bucket", bucket)
	values.Set("key", key)
	req, err := c.newRequest(ctx, http.MethodGet, "/_chunkgate/v1/manifest?"+values.Encode(), nil)
	if err != nil {
		return Manifest{}, err
	}
	req.Header.Set("Accept", "application/json")

	var manifest Manifest
	if err := c.doJSON(req, &manifest); err != nil {
		return Manifest{}, err
	}
	if manifest.Version != 1 {
		return Manifest{}, fmt.Errorf("unsupported manifest version %d", manifest.Version)
	}
	return manifest, nil
}

func (c Client) FetchChunks(ctx context.Context, bucket string, key string, hashes []string) (map[string][]byte, error) {
	request := struct {
		Bucket string   `json:"bucket"`
		Key    string   `json:"key"`
		Hashes []string `json:"hashes"`
	}{Bucket: bucket, Key: key, Hashes: hashes}
	body, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	req, err := c.newRequest(ctx, http.MethodPost, "/_chunkgate/v1/chunks", body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

	var response struct {
		Version int `json:"version"`
		Chunks  []struct {
			Hash string `json:"hash"`
			Size int64  `json:"size"`
			Data string `json:"data"`
		} `json:"chunks"`
	}
	if err := c.doJSON(req, &response); err != nil {
		return nil, err
	}
	if response.Version != 1 {
		return nil, fmt.Errorf("unsupported chunk response version %d", response.Version)
	}
	chunks := make(map[string][]byte, len(response.Chunks))
	for _, chunk := range response.Chunks {
		data, err := base64.StdEncoding.DecodeString(chunk.Data)
		if err != nil {
			return nil, fmt.Errorf("decode chunk %s: %w", chunk.Hash, err)
		}
		if int64(len(data)) != chunk.Size {
			return nil, fmt.Errorf("chunk %s size mismatch", chunk.Hash)
		}
		if sha256Hex(data) != chunk.Hash {
			return nil, fmt.Errorf("chunk %s hash mismatch", chunk.Hash)
		}
		chunks[chunk.Hash] = data
	}
	return chunks, nil
}

func (c Client) Download(ctx context.Context, bucket string, key string, outputPath string, cacheDir string) (DownloadResult, error) {
	if outputPath == "" {
		return DownloadResult{}, fmt.Errorf("output path must not be empty")
	}
	cache := Cache{Dir: cacheDir}
	manifest, err := c.FetchManifest(ctx, bucket, key)
	if err != nil {
		return DownloadResult{}, err
	}

	missing := make([]string, 0)
	seen := map[string]bool{}
	for _, chunk := range manifest.Chunks {
		if seen[chunk.Hash] {
			continue
		}
		seen[chunk.Hash] = true
		if !cache.Has(chunk.Hash, chunk.Size) {
			missing = append(missing, chunk.Hash)
		}
	}
	if len(missing) > 0 {
		chunks, err := c.FetchChunks(ctx, bucket, key, missing)
		if err != nil {
			return DownloadResult{}, err
		}
		for _, hash := range missing {
			data, ok := chunks[hash]
			if !ok {
				return DownloadResult{}, fmt.Errorf("server did not return requested chunk %s", hash)
			}
			if err := cache.Put(hash, data); err != nil {
				return DownloadResult{}, err
			}
		}
	}
	if err := reconstruct(ctx, manifest, cache, outputPath); err != nil {
		return DownloadResult{}, err
	}
	return DownloadResult{Bytes: manifest.Size, TotalChunks: len(manifest.Chunks), MissingChunks: len(missing)}, nil
}

func (c Client) newRequest(ctx context.Context, method string, companionPath string, body []byte) (*http.Request, error) {
	endpoint := strings.TrimRight(strings.TrimSpace(c.Endpoint), "/")
	if endpoint == "" {
		return nil, fmt.Errorf("endpoint must not be empty")
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint+companionPath, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if c.AccessKey != "" || c.SecretKey != "" {
		if c.AccessKey == "" || c.SecretKey == "" {
			return nil, fmt.Errorf("access key and secret key must both be set")
		}
		signRequest(req, body, c.AccessKey, c.SecretKey, c.region())
	}
	return req, nil
}

func (c Client) doJSON(req *http.Request, target any) error {
	client := c.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		return HTTPError{StatusCode: resp.StatusCode, Body: string(data)}
	}
	return json.NewDecoder(resp.Body).Decode(target)
}

func (c Client) region() string {
	if c.Region != "" {
		return c.Region
	}
	return "us-east-1"
}

func reconstruct(ctx context.Context, manifest Manifest, cache Cache, outputPath string) error {
	dir := filepath.Dir(outputPath)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create output directory: %w", err)
		}
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(outputPath)+".*.tmp")
	if err != nil {
		return fmt.Errorf("create output temp file: %w", err)
	}
	tmpName := tmp.Name()
	success := false
	defer func() {
		if !success {
			_ = os.Remove(tmpName)
		}
	}()

	hash := md5.New()
	var written int64
	for _, chunk := range manifest.Chunks {
		if err := ctx.Err(); err != nil {
			_ = tmp.Close()
			return err
		}
		data, err := cache.Read(chunk.Hash, chunk.Size)
		if err != nil {
			_ = tmp.Close()
			return err
		}
		n, err := tmp.Write(data)
		if err != nil {
			_ = tmp.Close()
			return fmt.Errorf("write output: %w", err)
		}
		if n != len(data) {
			_ = tmp.Close()
			return io.ErrShortWrite
		}
		if _, err := hash.Write(data); err != nil {
			_ = tmp.Close()
			return err
		}
		written += int64(len(data))
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close output temp file: %w", err)
	}
	if written != manifest.Size {
		return fmt.Errorf("reconstructed size %d does not match manifest size %d", written, manifest.Size)
	}
	if want := manifestObjectMD5(manifest); want != "" {
		got := hex.EncodeToString(hash.Sum(nil))
		if got != want {
			return fmt.Errorf("reconstructed object md5 %s does not match manifest md5 %s", got, want)
		}
	}
	if err := os.Rename(tmpName, outputPath); err != nil {
		_ = os.Remove(outputPath)
		if retryErr := os.Rename(tmpName, outputPath); retryErr != nil {
			return fmt.Errorf("commit output file: %w", err)
		}
	}
	success = true
	return nil
}

func manifestObjectMD5(manifest Manifest) string {
	if manifest.ObjectMD5 != "" {
		return strings.ToLower(manifest.ObjectMD5)
	}
	value := strings.ToLower(strings.Trim(strings.TrimSpace(manifest.ETag), `"`))
	if len(value) != 32 {
		return ""
	}
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'f':
		case r >= '0' && r <= '9':
		default:
			return ""
		}
	}
	return value
}

type Cache struct {
	Dir string
}

func (c Cache) Has(hash string, size int64) bool {
	_, err := c.Read(hash, size)
	return err == nil
}

func (c Cache) Read(hash string, size int64) ([]byte, error) {
	path, err := c.path(hash)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if size >= 0 && int64(len(data)) != size {
		return nil, fmt.Errorf("cached chunk %s size mismatch", hash)
	}
	if sha256Hex(data) != hash {
		return nil, fmt.Errorf("cached chunk %s hash mismatch", hash)
	}
	return data, nil
}

func (c Cache) Put(hash string, data []byte) error {
	if sha256Hex(data) != hash {
		return fmt.Errorf("downloaded chunk %s hash mismatch", hash)
	}
	if c.Has(hash, int64(len(data))) {
		return nil
	}
	path, err := c.path(hash)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create cache directory: %w", err)
	}
	tmp := path + "." + randomSuffix() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write cached chunk: %w", err)
	}
	_ = os.Remove(path)
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("commit cached chunk: %w", err)
	}
	return nil
}

func (c Cache) path(hash string) (string, error) {
	if !validChunkHash(hash) {
		return "", fmt.Errorf("invalid chunk hash %q", hash)
	}
	root := c.Dir
	if root == "" {
		root = ".chunkgate-cache"
	}
	path := filepath.Join(root, "blocks", hash[:2], hash)
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("resolve cache root: %w", err)
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve cache path: %w", err)
	}
	rel, err := filepath.Rel(absRoot, absPath)
	if err != nil {
		return "", fmt.Errorf("verify cache path: %w", err)
	}
	if rel == "." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." || filepath.IsAbs(rel) {
		return "", fmt.Errorf("invalid cache path")
	}
	return absPath, nil
}

func signRequest(req *http.Request, body []byte, accessKey string, secretKey string, region string) {
	now := time.Now().UTC()
	payloadHash := sha256Hex(body)
	req.Header.Set("X-Amz-Date", now.Format("20060102T150405Z"))
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	scope := now.Format("20060102") + "/" + region + "/s3/aws4_request"
	canonical := strings.Join([]string{
		req.Method,
		canonicalURI(req.URL),
		canonicalQueryString(req.URL),
		"host:" + req.Host + "\n" +
			"x-amz-content-sha256:" + req.Header.Get("X-Amz-Content-Sha256") + "\n" +
			"x-amz-date:" + req.Header.Get("X-Amz-Date") + "\n",
		signedHeaders,
		payloadHash,
	}, "\n")
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		req.Header.Get("X-Amz-Date"),
		scope,
		sha256Hex([]byte(canonical)),
	}, "\n")
	signingKey := deriveSigningKey(secretKey, now.Format("20060102"), region, "s3")
	signature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+accessKey+"/"+scope+", SignedHeaders="+signedHeaders+", Signature="+signature)
}

func validChunkHash(hash string) bool {
	if len(hash) != 64 {
		return false
	}
	for _, r := range hash {
		switch {
		case r >= 'a' && r <= 'f':
		case r >= '0' && r <= '9':
		default:
			return false
		}
	}
	return true
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func canonicalURI(u *url.URL) string {
	path := u.EscapedPath()
	if path == "" {
		return "/"
	}
	if !strings.HasPrefix(path, "/") {
		return "/" + path
	}
	return path
}

func canonicalQueryString(u *url.URL) string {
	values := u.Query()
	pairs := make([]string, 0)
	for name, vals := range values {
		if len(vals) == 0 {
			pairs = append(pairs, awsEncode(name)+"=")
			continue
		}
		for _, value := range vals {
			pairs = append(pairs, awsEncode(name)+"="+awsEncode(value))
		}
	}
	sort.Strings(pairs)
	return strings.Join(pairs, "&")
}

func awsEncode(value string) string {
	var b strings.Builder
	for i := 0; i < len(value); i++ {
		c := value[i]
		if (c >= 'A' && c <= 'Z') ||
			(c >= 'a' && c <= 'z') ||
			(c >= '0' && c <= '9') ||
			c == '-' || c == '_' || c == '.' || c == '~' {
			b.WriteByte(c)
			continue
		}
		b.WriteString("%" + strings.ToUpper(hex.EncodeToString([]byte{c})))
	}
	return b.String()
}

func deriveSigningKey(secret string, date string, region string, service string) []byte {
	dateKey := hmacSHA256([]byte("AWS4"+secret), []byte(date))
	regionKey := hmacSHA256(dateKey, []byte(region))
	serviceKey := hmacSHA256(regionKey, []byte(service))
	return hmacSHA256(serviceKey, []byte("aws4_request"))
}

func hmacSHA256(key []byte, payload []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(payload)
	return mac.Sum(nil)
}

func randomSuffix() string {
	var bytes [8]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "fallback"
	}
	return hex.EncodeToString(bytes[:])
}
