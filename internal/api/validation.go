package api

import (
	"net"
	"strconv"
	"strings"
	"unicode/utf8"
)

const maxS3PartNumber = 10000

func validateBucketName(bucket string) bool {
	if len(bucket) < 3 || len(bucket) > 63 {
		return false
	}
	if net.ParseIP(bucket) != nil {
		return false
	}
	previousDot := false
	for i, r := range bucket {
		valid := r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '.' || r == '-'
		if !valid {
			return false
		}
		if i == 0 && !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9') {
			return false
		}
		if r == '.' && previousDot {
			return false
		}
		previousDot = r == '.'
	}
	last := bucket[len(bucket)-1]
	if !(last >= 'a' && last <= 'z' || last >= '0' && last <= '9') {
		return false
	}
	return !strings.Contains(bucket, ".-") && !strings.Contains(bucket, "-.")
}

func validateObjectKey(key string) bool {
	if key == "" || len(key) > 1024 || !utf8.ValidString(key) {
		return false
	}
	for _, r := range key {
		if r == 0 || r < 0x20 && r != '\t' {
			return false
		}
	}
	return true
}

func validateUploadID(uploadID string) bool {
	if len(uploadID) != 32 {
		return false
	}
	for _, r := range uploadID {
		switch {
		case r >= 'a' && r <= 'f':
		case r >= '0' && r <= '9':
		default:
			return false
		}
	}
	return true
}

func parsePartNumber(raw string) (int, bool) {
	if raw == "" {
		return 0, false
	}
	for _, r := range raw {
		if r < '0' || r > '9' {
			return 0, false
		}
	}
	partNumber, err := strconv.Atoi(raw)
	if err != nil || partNumber <= 0 || partNumber > maxS3PartNumber {
		return 0, false
	}
	return partNumber, true
}
