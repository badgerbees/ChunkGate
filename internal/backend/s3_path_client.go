package backend

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

type pathS3Options struct {
	Endpoint     normalizedS3Endpoint
	Region       string
	AccessKey    string
	SecretKey    string
	SessionToken string
	PathStyle    bool
}

type pathS3Client struct {
	endpoint     normalizedS3Endpoint
	region       string
	accessKey    string
	secretKey    string
	sessionToken string
	pathStyle    bool
	httpClient   *http.Client
	now          func() time.Time
}

func newPathS3Client(options pathS3Options) *pathS3Client {
	region := strings.TrimSpace(options.Region)
	if region == "" {
		region = "us-east-1"
	}
	return &pathS3Client{
		endpoint:     options.Endpoint,
		region:       region,
		accessKey:    options.AccessKey,
		secretKey:    options.SecretKey,
		sessionToken: options.SessionToken,
		pathStyle:    options.PathStyle,
		httpClient:   http.DefaultClient,
		now:          time.Now,
	}
}

func (c *pathS3Client) PutObject(ctx context.Context, bucket string, key string, data []byte) error {
	resp, err := c.do(ctx, http.MethodPut, bucket, key, nil, data, map[string]string{
		"Content-Type": "application/octet-stream",
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return s3ResponseError(resp)
}

func (c *pathS3Client) GetObject(ctx context.Context, bucket string, key string) (io.ReadCloser, error) {
	resp, err := c.do(ctx, http.MethodGet, bucket, key, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp.Body, nil
	}
	defer resp.Body.Close()
	return nil, s3ResponseError(resp)
}

func (c *pathS3Client) StatObject(ctx context.Context, bucket string, key string) error {
	resp, err := c.do(ctx, http.MethodHead, bucket, key, nil, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return s3ResponseError(resp)
}

func (c *pathS3Client) DeleteObjects(ctx context.Context, bucket string, keys []string) error {
	body, err := deleteObjectsBody(keys)
	if err != nil {
		return err
	}
	sum := md5.Sum(body)
	resp, err := c.do(ctx, http.MethodPost, bucket, "", url.Values{"delete": {""}}, body, map[string]string{
		"Content-MD5":  base64.StdEncoding.EncodeToString(sum[:]),
		"Content-Type": "application/xml",
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := s3ResponseError(resp); err != nil {
		return err
	}
	var result deleteResult
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil && !errorsIsEOF(err) {
		return err
	}
	for _, item := range result.Errors {
		if item.Code != "NoSuchKey" && item.Code != "NoSuchObject" {
			return s3HTTPError{StatusCode: http.StatusBadRequest, Code: item.Code, Message: item.Message}
		}
	}
	return nil
}

func (c *pathS3Client) BucketExists(ctx context.Context, bucket string) (bool, error) {
	resp, err := c.do(ctx, http.MethodHead, bucket, "", nil, nil, nil)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return true, nil
	}
	err = s3ResponseError(resp)
	if isS3NotFound(err) || isS3NoSuchBucket(err) {
		return false, nil
	}
	return false, err
}

func (c *pathS3Client) do(ctx context.Context, method string, bucket string, key string, query url.Values, body []byte, headers map[string]string) (*http.Response, error) {
	target := c.objectURL(bucket, key)
	if len(query) > 0 {
		target.RawQuery = query.Encode()
	}
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, target.String(), reader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.ContentLength = int64(len(body))
	}
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	payloadHash := sha256Hex(body)
	req.Header.Set("x-amz-content-sha256", payloadHash)
	req.Header.Set("User-Agent", "chunkgate")
	c.sign(req, payloadHash, c.now().UTC())
	return c.httpClient.Do(req)
}

func (c *pathS3Client) objectURL(bucket string, key string) *url.URL {
	scheme := "http"
	if c.endpoint.Secure {
		scheme = "https"
	}
	host := c.endpoint.Host
	pathParts := []string{c.endpoint.BasePath}
	if c.pathStyle {
		pathParts = append(pathParts, bucket)
	} else if bucket != "" {
		host = bucket + "." + host
	}
	if key != "" {
		pathParts = append(pathParts, key)
	}
	return &url.URL{
		Scheme: scheme,
		Host:   host,
		Path:   joinS3Path(pathParts...),
	}
}

func (c *pathS3Client) sign(req *http.Request, payloadHash string, now time.Time) {
	if c.accessKey == "" && c.secretKey == "" {
		return
	}
	amzDate := now.Format("20060102T150405Z")
	date := now.Format("20060102")
	req.Header.Set("x-amz-date", amzDate)
	if c.sessionToken != "" {
		req.Header.Set("x-amz-security-token", c.sessionToken)
	}

	signedHeaders := signedHeaderNames(req)
	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI(req.URL),
		canonicalQuery(req.URL.Query()),
		canonicalHeaders(req, signedHeaders),
		strings.Join(signedHeaders, ";"),
		payloadHash,
	}, "\n")
	scope := date + "/" + c.region + "/s3/aws4_request"
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")
	signingKey := sigV4SigningKey(c.secretKey, date, c.region, "s3")
	signature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+c.accessKey+"/"+scope+", SignedHeaders="+strings.Join(signedHeaders, ";")+", Signature="+signature)
}

func signedHeaderNames(req *http.Request) []string {
	names := []string{"host"}
	for name := range req.Header {
		lower := strings.ToLower(name)
		if strings.HasPrefix(lower, "x-amz-") || lower == "content-md5" {
			names = append(names, lower)
		}
	}
	sort.Strings(names[1:])
	return names
}

func canonicalHeaders(req *http.Request, signedHeaders []string) string {
	var b strings.Builder
	for _, name := range signedHeaders {
		value := req.Host
		if name != "host" {
			value = strings.Join(req.Header.Values(name), ",")
		} else if value == "" {
			value = req.URL.Host
		}
		b.WriteString(name)
		b.WriteByte(':')
		b.WriteString(canonicalHeaderValue(value))
		b.WriteByte('\n')
	}
	return b.String()
}

func canonicalHeaderValue(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

func canonicalURI(u *url.URL) string {
	escaped := u.EscapedPath()
	if escaped == "" {
		return "/"
	}
	return escaped
}

func canonicalQuery(values url.Values) string {
	if len(values) == 0 {
		return ""
	}
	pairs := make([]string, 0)
	for name, vals := range values {
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
		if c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.' || c == '~' {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('%')
		b.WriteString(strings.ToUpper(hex.EncodeToString([]byte{c})))
	}
	return b.String()
}

func sigV4SigningKey(secret string, date string, region string, service string) []byte {
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

func sha256Hex(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func joinS3Path(parts ...string) string {
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.Trim(part, "/")
		if part != "" {
			out = append(out, part)
		}
	}
	if len(out) == 0 {
		return "/"
	}
	return "/" + strings.Join(out, "/")
}

func deleteObjectsBody(keys []string) ([]byte, error) {
	var body bytes.Buffer
	body.WriteString(xml.Header)
	encoder := xml.NewEncoder(&body)
	if err := encoder.Encode(deleteRequest{Objects: deleteRequestObjects(keys)}); err != nil {
		return nil, err
	}
	return body.Bytes(), nil
}

func deleteRequestObjects(keys []string) []deleteObject {
	objects := make([]deleteObject, 0, len(keys))
	for _, key := range keys {
		objects = append(objects, deleteObject{Key: key})
	}
	return objects
}

func s3ResponseError(resp *http.Response) error {
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	err := s3HTTPError{StatusCode: resp.StatusCode, Code: resp.Status}
	if resp.Body != nil && resp.ContentLength != 0 {
		var response struct {
			Code    string `xml:"Code"`
			Message string `xml:"Message"`
		}
		if decodeErr := xml.NewDecoder(resp.Body).Decode(&response); decodeErr == nil {
			err.Code = response.Code
			err.Message = response.Message
		}
	}
	return err
}

func errorsIsEOF(err error) bool {
	return err == nil || err == io.EOF
}

type s3HTTPError struct {
	StatusCode int
	Code       string
	Message    string
}

func (e s3HTTPError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("s3 status %d %s", e.StatusCode, e.Code)
	}
	return fmt.Sprintf("s3 status %d %s: %s", e.StatusCode, e.Code, e.Message)
}

type deleteRequest struct {
	XMLName xml.Name       `xml:"Delete"`
	Objects []deleteObject `xml:"Object"`
}

type deleteObject struct {
	Key string `xml:"Key"`
}

type deleteResult struct {
	Errors []deleteError `xml:"Error"`
}

type deleteError struct {
	Key     string `xml:"Key"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}
