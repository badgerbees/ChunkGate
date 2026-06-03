package chunker

import (
	"bytes"
	"context"
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
