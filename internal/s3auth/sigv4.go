package s3auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

const algorithm = "AWS4-HMAC-SHA256"

type Credential struct {
	AccessKey string
	SecretKey string
	Tenant    string
}

type Identity struct {
	AccessKey string
	Tenant    string
}

type Error struct {
	Status  int
	Code    string
	Message string
}

func (e *Error) Error() string {
	return e.Code + ": " + e.Message
}

type Verifier struct {
	credentials map[string]Credential
	Now         func() time.Time
	MaxSkew     time.Duration
}

func NewVerifier(credentials []Credential) (*Verifier, error) {
	verifier := &Verifier{
		credentials: map[string]Credential{},
		Now:         time.Now,
		MaxSkew:     15 * time.Minute,
	}
	for _, credential := range credentials {
		if credential.AccessKey == "" || credential.SecretKey == "" {
			return nil, fmt.Errorf("access key and secret key must both be set")
		}
		if credential.Tenant == "" {
			credential.Tenant = credential.AccessKey
		}
		verifier.credentials[credential.AccessKey] = credential
	}
	return verifier, nil
}

func (v *Verifier) Enabled() bool {
	return v != nil && len(v.credentials) > 0
}

func (v *Verifier) Verify(r *http.Request) (Identity, error) {
	if !v.Enabled() {
		return Identity{Tenant: "default"}, nil
	}

	authorization := r.Header.Get("Authorization")
	if authorization == "" {
		return Identity{}, authError(http.StatusForbidden, "AccessDenied", "missing authorization header")
	}

	parsed, err := parseAuthorization(authorization)
	if err != nil {
		return Identity{}, err
	}
	credential, ok := v.credentials[parsed.accessKey]
	if !ok {
		return Identity{}, authError(http.StatusForbidden, "InvalidAccessKeyId", "the AWS access key ID does not exist")
	}

	requestTime, err := time.Parse("20060102T150405Z", r.Header.Get("X-Amz-Date"))
	if err != nil {
		return Identity{}, authError(http.StatusBadRequest, "AuthorizationHeaderMalformed", "missing or invalid x-amz-date")
	}
	if v.MaxSkew > 0 {
		now := v.Now
		if now == nil {
			now = time.Now
		}
		delta := now().UTC().Sub(requestTime.UTC())
		if delta < -v.MaxSkew || delta > v.MaxSkew {
			return Identity{}, authError(http.StatusForbidden, "RequestTimeTooSkewed", "the difference between the request time and server time is too large")
		}
	}
	if parsed.date != requestTime.Format("20060102") {
		return Identity{}, authError(http.StatusBadRequest, "AuthorizationHeaderMalformed", "credential scope date does not match x-amz-date")
	}

	canonicalRequest, err := canonicalRequest(r, parsed.signedHeaders)
	if err != nil {
		return Identity{}, err
	}
	scope := strings.Join([]string{parsed.date, parsed.region, parsed.service, "aws4_request"}, "/")
	stringToSign := strings.Join([]string{
		algorithm,
		r.Header.Get("X-Amz-Date"),
		scope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")
	signingKey := deriveSigningKey(credential.SecretKey, parsed.date, parsed.region, parsed.service)
	expected := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))
	if !constantTimeHexEqual(expected, parsed.signature) {
		return Identity{}, authError(http.StatusForbidden, "SignatureDoesNotMatch", "the request signature we calculated does not match the signature you provided")
	}

	return Identity{AccessKey: credential.AccessKey, Tenant: credential.Tenant}, nil
}

type parsedAuthorization struct {
	accessKey     string
	date          string
	region        string
	service       string
	signedHeaders string
	signature     string
}

func parseAuthorization(header string) (parsedAuthorization, error) {
	if !strings.HasPrefix(header, algorithm+" ") {
		return parsedAuthorization{}, authError(http.StatusBadRequest, "AuthorizationHeaderMalformed", "unsupported authorization algorithm")
	}
	fields := map[string]string{}
	for _, field := range strings.Split(strings.TrimPrefix(header, algorithm+" "), ",") {
		name, value, ok := strings.Cut(strings.TrimSpace(field), "=")
		if !ok || name == "" || value == "" {
			return parsedAuthorization{}, authError(http.StatusBadRequest, "AuthorizationHeaderMalformed", "invalid authorization field")
		}
		fields[name] = value
	}

	credentialScope := strings.Split(fields["Credential"], "/")
	if len(credentialScope) != 5 || credentialScope[4] != "aws4_request" {
		return parsedAuthorization{}, authError(http.StatusBadRequest, "AuthorizationHeaderMalformed", "invalid credential scope")
	}
	if fields["SignedHeaders"] == "" || fields["Signature"] == "" {
		return parsedAuthorization{}, authError(http.StatusBadRequest, "AuthorizationHeaderMalformed", "authorization header is missing signed headers or signature")
	}
	return parsedAuthorization{
		accessKey:     credentialScope[0],
		date:          credentialScope[1],
		region:        credentialScope[2],
		service:       credentialScope[3],
		signedHeaders: fields["SignedHeaders"],
		signature:     fields["Signature"],
	}, nil
}

func canonicalRequest(r *http.Request, signedHeaders string) (string, error) {
	headers, err := canonicalHeaders(r, signedHeaders)
	if err != nil {
		return "", err
	}
	return strings.Join([]string{
		r.Method,
		canonicalURI(r.URL),
		canonicalQueryString(r.URL),
		headers,
		signedHeaders,
		payloadHash(r),
	}, "\n"), nil
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

func canonicalHeaders(r *http.Request, signedHeaders string) (string, error) {
	var b strings.Builder
	for _, name := range strings.Split(signedHeaders, ";") {
		name = strings.ToLower(strings.TrimSpace(name))
		if name == "" {
			return "", authError(http.StatusBadRequest, "AuthorizationHeaderMalformed", "signed headers contains an empty header name")
		}
		value := ""
		if name == "host" {
			value = r.Host
		} else {
			values := r.Header.Values(name)
			if len(values) == 0 {
				return "", authError(http.StatusBadRequest, "SignatureDoesNotMatch", "a signed header is missing from the request")
			}
			value = strings.Join(values, ",")
		}
		b.WriteString(name)
		b.WriteByte(':')
		b.WriteString(collapseSpaces(value))
		b.WriteByte('\n')
	}
	return b.String(), nil
}

func payloadHash(r *http.Request) string {
	hash := r.Header.Get("X-Amz-Content-Sha256")
	if hash == "" {
		return "UNSIGNED-PAYLOAD"
	}
	return hash
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

func sha256Hex(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func constantTimeHexEqual(expected string, actual string) bool {
	expectedBytes, err := hex.DecodeString(expected)
	if err != nil {
		return false
	}
	actualBytes, err := hex.DecodeString(actual)
	if err != nil {
		return false
	}
	return subtle.ConstantTimeCompare(expectedBytes, actualBytes) == 1
}

func collapseSpaces(value string) string {
	return strings.Join(strings.Fields(value), " ")
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
		b.WriteString(fmt.Sprintf("%%%02X", c))
	}
	return b.String()
}

func authError(status int, code string, message string) *Error {
	return &Error{Status: status, Code: code, Message: message}
}
