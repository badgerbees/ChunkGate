package api

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"net/http"

	"github.com/chunkgate/chunkgate/internal/backend"
	"github.com/chunkgate/chunkgate/internal/limits"
)

func addCommonHeaders(w http.ResponseWriter, requestID string) {
	if requestID == "" {
		requestID = "chunkgate"
	}
	w.Header().Set("x-amz-request-id", requestID)
}

func writeXML(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	_ = xml.NewEncoder(w).Encode(value)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code string, message string) {
	requestID := w.Header().Get("x-amz-request-id")
	if requestID == "" {
		requestID = "chunkgate"
	}
	writeXML(w, status, errorResponse{Code: code, Message: message, RequestID: requestID})
}

func writeInternalError(w http.ResponseWriter, err error) {
	if errors.Is(err, backend.ErrBlockNotFound) {
		writeError(w, http.StatusNotFound, "NoSuchKey", "the specified key does not exist")
		return
	}
	if isCapacityError(err) {
		writeCapacityError(w)
		return
	}
	if errors.Is(err, backend.ErrBackendUnavailable) {
		writeError(w, http.StatusServiceUnavailable, "ServiceUnavailable", "the storage backend is temporarily unavailable")
		return
	}
	writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
}

func writeCapacityError(w http.ResponseWriter) {
	writeError(w, http.StatusInsufficientStorage, "InsufficientStorage", "the server does not have enough reserved scratch capacity or free disk space")
}

func isCapacityError(err error) bool {
	return errors.Is(err, limits.ErrCapacityExceeded) || errors.Is(err, limits.ErrInsufficientDisk)
}

func isBodyLimitError(err error) bool {
	var maxErr *http.MaxBytesError
	return errors.As(err, &maxErr)
}

type s3Error struct {
	Status  int
	Code    string
	Message string
}

func (e s3Error) Error() string {
	return e.Code + ": " + e.Message
}
