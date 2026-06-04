package api

import (
	"encoding/xml"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/chunkgate/chunkgate/internal/multipart"
	"github.com/chunkgate/chunkgate/internal/object"
)

func (s *Server) initiateMultipart(w http.ResponseWriter, r *http.Request, tenant string, bucket string, key string) {
	done, ok := s.beginUpload(w)
	if !ok {
		return
	}
	success := false
	defer func() {
		done(success)
	}()

	expected := parseInt64Header(r, "X-ChunkGate-Expected-Size")
	session, err := s.multipart.Create(r.Context(), tenant, bucket, key, expected, extractObjectHeaders(r.Header))
	if errors.Is(err, multipart.ErrUploadTooLarge) {
		writeError(w, http.StatusBadRequest, "EntityTooLarge", "the proposed multipart upload exceeds the configured maximum size")
		return
	}
	if isCapacityError(err) {
		writeCapacityError(w)
		return
	}
	if err != nil {
		writeInternalError(w, err)
		return
	}
	success = true
	writeXML(w, http.StatusOK, initiateMultipartUploadResult{
		Bucket:   bucket,
		Key:      key,
		UploadID: session.UploadID,
	})
}

func (s *Server) uploadPart(w http.ResponseWriter, r *http.Request, tenant string) {
	done, ok := s.beginUpload(w)
	if !ok {
		return
	}
	success := false
	defer func() {
		done(success)
	}()

	uploadID := r.URL.Query().Get("uploadId")
	number, err := strconv.Atoi(r.URL.Query().Get("partNumber"))
	if err != nil || number <= 0 {
		writeError(w, http.StatusBadRequest, "InvalidPart", "partNumber must be a positive integer")
		return
	}
	if s.maxPartBytes > 0 && r.ContentLength > s.maxPartBytes {
		writeError(w, http.StatusBadRequest, "EntityTooLarge", "the uploaded part exceeds the configured multipart size limit")
		return
	}
	if r.ContentLength > 0 {
		if err := s.multipart.CheckPart(r.Context(), tenant, uploadID, number, r.ContentLength); err != nil {
			if errors.Is(err, multipart.ErrUploadNotFound) {
				writeError(w, http.StatusNotFound, "NoSuchUpload", "the specified multipart upload does not exist")
				return
			}
			if errors.Is(err, multipart.ErrPartTooLarge) || errors.Is(err, multipart.ErrUploadTooLarge) {
				writeError(w, http.StatusBadRequest, "EntityTooLarge", "the uploaded part exceeds the configured multipart size limit")
				return
			}
			if isCapacityError(err) {
				writeCapacityError(w)
				return
			}
			writeInternalError(w, err)
			return
		}
	}
	body, ok := s.limitedBody(w, r, s.maxPartBytes)
	if !ok {
		return
	}
	part, err := s.multipart.PutPart(r.Context(), tenant, uploadID, number, body)
	if errors.Is(err, multipart.ErrUploadNotFound) {
		writeError(w, http.StatusNotFound, "NoSuchUpload", "the specified multipart upload does not exist")
		return
	}
	if errors.Is(err, multipart.ErrPartTooLarge) || errors.Is(err, multipart.ErrUploadTooLarge) {
		writeError(w, http.StatusBadRequest, "EntityTooLarge", "the uploaded part exceeds the configured multipart size limit")
		return
	}
	if isBodyLimitError(err) {
		writeError(w, http.StatusBadRequest, "EntityTooLarge", "the uploaded part exceeds the configured multipart size limit")
		return
	}
	if isCapacityError(err) {
		writeCapacityError(w)
		return
	}
	if err != nil {
		writeInternalError(w, err)
		return
	}
	success = true
	w.Header().Set("ETag", part.ETag)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) completeMultipart(w http.ResponseWriter, r *http.Request, tenant string) {
	done, ok := s.beginUpload(w)
	if !ok {
		return
	}
	success := false
	defer func() {
		done(success)
	}()

	uploadID := r.URL.Query().Get("uploadId")
	var request completeMultipartUpload
	body, ok := s.limitedBody(w, r, s.maxCompleteXML)
	if !ok {
		return
	}
	if err := xml.NewDecoder(body).Decode(&request); err != nil {
		if isBodyLimitError(err) {
			writeError(w, http.StatusBadRequest, "EntityTooLarge", "the complete multipart payload is too large")
			return
		}
		writeError(w, http.StatusBadRequest, "MalformedXML", "the XML you provided was not well-formed or did not validate against our published schema")
		return
	}

	session, err := s.multipart.Get(r.Context(), tenant, uploadID)
	if errors.Is(err, multipart.ErrUploadNotFound) {
		writeError(w, http.StatusNotFound, "NoSuchUpload", "the specified multipart upload does not exist")
		return
	}
	if err != nil {
		writeInternalError(w, err)
		return
	}
	order, err := validateCompletedParts(session.Parts, request.Parts)
	if err != nil {
		var apiErr s3Error
		if errors.As(err, &apiErr) {
			writeError(w, apiErr.Status, apiErr.Code, apiErr.Message)
			return
		}
		writeInternalError(w, err)
		return
	}

	_, reader, err := s.multipart.Open(r.Context(), tenant, uploadID, order)
	if err != nil {
		writeInternalError(w, err)
		return
	}

	manifest, err := s.objects.Put(r.Context(), tenant, session.Bucket, session.Key, reader, object.WithHeaders(session.Headers))
	closeErr := reader.Close()
	if isBodyLimitError(err) {
		writeError(w, http.StatusBadRequest, "EntityTooLarge", "the complete multipart payload is too large")
		return
	}
	if err != nil {
		writeInternalError(w, err)
		return
	}
	if closeErr != nil {
		writeInternalError(w, closeErr)
		return
	}
	if err := s.multipart.CompleteCleanup(r.Context(), tenant, uploadID); err != nil {
		writeInternalError(w, err)
		return
	}

	success = true
	writeXML(w, http.StatusOK, completeMultipartUploadResult{
		Location: location(session.Bucket, session.Key),
		Bucket:   session.Bucket,
		Key:      session.Key,
		ETag:     manifest.ETag,
	})
}

func (s *Server) abortMultipart(w http.ResponseWriter, r *http.Request, tenant string) {
	if err := s.multipart.Abort(r.Context(), tenant, r.URL.Query().Get("uploadId")); errors.Is(err, multipart.ErrUploadNotFound) {
		writeError(w, http.StatusNotFound, "NoSuchUpload", "the specified multipart upload does not exist")
		return
	} else if err != nil {
		writeInternalError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func validateCompletedParts(uploaded map[int]multipart.PartInfo, completed []completePart) ([]int, error) {
	if len(completed) == 0 {
		return nil, s3Error{Status: http.StatusBadRequest, Code: "MalformedXML", Message: "complete multipart upload requires at least one part"}
	}
	order := make([]int, 0, len(completed))
	seen := map[int]bool{}
	last := 0
	for _, part := range completed {
		if part.PartNumber <= 0 {
			return nil, s3Error{Status: http.StatusBadRequest, Code: "InvalidPart", Message: "part numbers must be positive"}
		}
		if part.PartNumber <= last {
			return nil, s3Error{Status: http.StatusBadRequest, Code: "InvalidPartOrder", Message: "the list of parts was not in ascending order"}
		}
		if seen[part.PartNumber] {
			return nil, s3Error{Status: http.StatusBadRequest, Code: "InvalidPart", Message: "duplicate part number in complete multipart upload"}
		}
		uploadedPart, ok := uploaded[part.PartNumber]
		if !ok {
			return nil, s3Error{Status: http.StatusBadRequest, Code: "InvalidPart", Message: "one or more of the specified parts could not be found"}
		}
		if normalizeETag(part.ETag) == "" || normalizeETag(part.ETag) != normalizeETag(uploadedPart.ETag) {
			return nil, s3Error{Status: http.StatusBadRequest, Code: "InvalidPart", Message: "one or more of the specified parts had an invalid ETag"}
		}
		seen[part.PartNumber] = true
		last = part.PartNumber
		order = append(order, part.PartNumber)
	}
	return order, nil
}

func normalizeETag(etag string) string {
	return strings.ToLower(strings.Trim(strings.TrimSpace(etag), `"`))
}
