package api

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/chunkgate/chunkgate/internal/metadata"
	"github.com/chunkgate/chunkgate/internal/object"
)

func applyRangeHeaders(w http.ResponseWriter, manifest metadata.ObjectManifest, byteRange object.ByteRange, length int64) {
	applyObjectHeaders(w, manifest.Headers)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("ETag", manifest.ETag)
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", byteRange.Start, byteRange.End, manifest.Size))
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
}

func parseSingleRange(header string, size int64) (object.ByteRange, error) {
	if !strings.HasPrefix(header, "bytes=") {
		return object.ByteRange{}, fmt.Errorf("only bytes ranges are supported")
	}
	spec := strings.TrimSpace(strings.TrimPrefix(header, "bytes="))
	if spec == "" {
		return object.ByteRange{}, fmt.Errorf("range is empty")
	}
	if strings.Contains(spec, ",") {
		return object.ByteRange{}, fmt.Errorf("multiple ranges are not supported")
	}
	startRaw, endRaw, ok := strings.Cut(spec, "-")
	if !ok {
		return object.ByteRange{}, fmt.Errorf("range must include a dash")
	}
	startRaw = strings.TrimSpace(startRaw)
	endRaw = strings.TrimSpace(endRaw)
	if startRaw == "" && endRaw == "" {
		return object.ByteRange{}, fmt.Errorf("range is empty")
	}
	if size <= 0 {
		return object.ByteRange{}, fmt.Errorf("range is not satisfiable")
	}

	if startRaw == "" {
		suffixLength, err := strconv.ParseInt(endRaw, 10, 64)
		if err != nil || suffixLength <= 0 {
			return object.ByteRange{}, fmt.Errorf("suffix length is invalid")
		}
		start := size - suffixLength
		if start < 0 {
			start = 0
		}
		return object.ByteRange{Start: start, End: size - 1}, nil
	}

	start, err := strconv.ParseInt(startRaw, 10, 64)
	if err != nil || start < 0 {
		return object.ByteRange{}, fmt.Errorf("range start is invalid")
	}
	if start >= size {
		return object.ByteRange{}, fmt.Errorf("range is not satisfiable")
	}
	if endRaw == "" {
		return object.ByteRange{Start: start, End: size - 1}, nil
	}

	end, err := strconv.ParseInt(endRaw, 10, 64)
	if err != nil || end < 0 {
		return object.ByteRange{}, fmt.Errorf("range end is invalid")
	}
	if end < start {
		return object.ByteRange{}, fmt.Errorf("range end is before range start")
	}
	if end >= size {
		end = size - 1
	}
	return object.ByteRange{Start: start, End: end}, nil
}
