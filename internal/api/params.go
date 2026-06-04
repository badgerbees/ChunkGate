package api

import (
	"net/http"
	"strconv"
)

func parseInt64Header(r *http.Request, key string) int64 {
	value := r.Header.Get(key)
	if value == "" {
		return 0
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 0 {
		return 0
	}
	return parsed
}
