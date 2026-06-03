package chunker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"testing"
)

func TestSmallFileBypassProducesSingleChunk(t *testing.T) {
	splitter := New(Options{MinSize: 4, AvgSize: 8, MaxSize: 16, SmallFileThreshold: 32})
	chunks, err := splitter.Split(context.Background(), bytes.NewReader([]byte("small payload")))
	if err != nil {
		t.Fatalf("split failed: %v", err)
	}
	if len(chunks) != 1 {
		t.Fatalf("expected one chunk, got %d", len(chunks))
	}
	if string(chunks[0].Data) != "small payload" {
		t.Fatalf("unexpected chunk data: %q", chunks[0].Data)
	}
}

func TestChunkBoundariesStayWithinConfiguredLimits(t *testing.T) {
	data := bytes.Repeat([]byte("abcdefghijklmnopqrstuvwxyz"), 100)
	splitter := New(Options{MinSize: 64, AvgSize: 128, MaxSize: 256, SmallFileThreshold: 0})
	chunks, err := splitter.Split(context.Background(), bytes.NewReader(data))
	if err != nil {
		t.Fatalf("split failed: %v", err)
	}
	if len(chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(chunks))
	}

	var total int
	for i, chunk := range chunks {
		total += len(chunk.Data)
		if i < len(chunks)-1 && len(chunk.Data) < 64 {
			t.Fatalf("chunk %d below minimum: %d", i, len(chunk.Data))
		}
		if len(chunk.Data) > 256 {
			t.Fatalf("chunk %d above maximum: %d", i, len(chunk.Data))
		}
		if chunk.Offset != int64(total-len(chunk.Data)) {
			t.Fatalf("chunk %d offset = %d, want %d", i, chunk.Offset, total-len(chunk.Data))
		}
	}
	if total != len(data) {
		t.Fatalf("reassembled size = %d, want %d", total, len(data))
	}
}

func TestStreamUsesBoundedReadBuffersAndChunkSizes(t *testing.T) {
	reader := &generatedReader{remaining: 8 * 1024 * 1024}
	splitter := New(Options{MinSize: 64 * 1024, AvgSize: 128 * 1024, MaxSize: 256 * 1024, SmallFileThreshold: 0})

	var total int64
	var chunks int
	err := splitter.Stream(context.Background(), reader, func(chunk Chunk) error {
		chunks++
		total += int64(len(chunk.Data))
		if len(chunk.Data) > 256*1024 {
			t.Fatalf("chunk size = %d, want <= 256KiB", len(chunk.Data))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("stream failed: %v", err)
	}
	if total != 8*1024*1024 {
		t.Fatalf("total = %d, want 8MiB", total)
	}
	if chunks < 2 {
		t.Fatalf("expected multiple chunks, got %d", chunks)
	}
	if reader.maxReadRequest > 32*1024 {
		t.Fatalf("max read request = %d, want <= 32KiB", reader.maxReadRequest)
	}
}

func TestStreamStopsWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	splitter := New(Options{MinSize: 64, AvgSize: 128, MaxSize: 256, SmallFileThreshold: 0})
	var emitted int
	err := splitter.Stream(ctx, bytes.NewReader(bytes.Repeat([]byte("a"), 4096)), func(chunk Chunk) error {
		emitted++
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context canceled", err)
	}
	if emitted != 1 {
		t.Fatalf("emitted = %d, want 1", emitted)
	}
}

func TestStreamChunkBoundariesAreStable(t *testing.T) {
	data := bytes.Repeat([]byte("the quick brown fox jumps over the lazy dog"), 1024)
	splitter := New(Options{MinSize: 512, AvgSize: 1024, MaxSize: 4096, SmallFileThreshold: 0})

	first := streamFingerprints(t, splitter, data)
	second := streamFingerprints(t, splitter, data)
	if len(first) != len(second) {
		t.Fatalf("chunk count = %d, want %d", len(second), len(first))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("chunk %d fingerprint = %s, want %s", i, second[i], first[i])
		}
	}
}

type generatedReader struct {
	remaining      int
	value          byte
	maxReadRequest int
}

func (r *generatedReader) Read(p []byte) (int, error) {
	if len(p) > r.maxReadRequest {
		r.maxReadRequest = len(p)
	}
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := len(p)
	if n > r.remaining {
		n = r.remaining
	}
	for i := 0; i < n; i++ {
		p[i] = r.value
		r.value++
	}
	r.remaining -= n
	return n, nil
}

func streamFingerprints(t *testing.T, splitter *Splitter, data []byte) []string {
	t.Helper()
	var fingerprints []string
	err := splitter.Stream(context.Background(), bytes.NewReader(data), func(chunk Chunk) error {
		sum := sha256.Sum256(chunk.Data)
		fingerprints = append(fingerprints, hex.EncodeToString(sum[:])+":"+intString(chunk.Offset)+":"+intString(int64(len(chunk.Data))))
		return nil
	})
	if err != nil {
		t.Fatalf("stream failed: %v", err)
	}
	return fingerprints
}

func intString(value int64) string {
	if value == 0 {
		return "0"
	}
	var digits [20]byte
	i := len(digits)
	negative := value < 0
	if negative {
		value = -value
	}
	for value > 0 {
		i--
		digits[i] = byte('0' + value%10)
		value /= 10
	}
	if negative {
		i--
		digits[i] = '-'
	}
	return string(digits[i:])
}
