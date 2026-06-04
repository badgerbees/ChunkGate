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
	if reader.maxReadRequest > 256*1024 {
		t.Fatalf("max read request = %d, want <= max chunk size", reader.maxReadRequest)
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

func TestFastCDCGoldenBoundaries(t *testing.T) {
	data := makeBenchmarkData(64 * 1024)
	splitter := New(Options{MinSize: 512, AvgSize: 1024, MaxSize: 4096, SmallFileThreshold: 0, Engine: EngineFastCDC})
	got := collectSummaries(t, splitter, data)
	want := []chunkSummary{
		{Offset: 0, Size: 881, SHA256: "e828efb375f9d552"},
		{Offset: 881, Size: 1044, SHA256: "1f267d6a258f6023"},
		{Offset: 1925, Size: 3708, SHA256: "96019edf3bccaa7a"},
		{Offset: 5633, Size: 1059, SHA256: "1a5ff8afa1e576e9"},
		{Offset: 6692, Size: 2037, SHA256: "b0e3d85e16a1e803"},
		{Offset: 8729, Size: 554, SHA256: "03006162c0c9da76"},
		{Offset: 9283, Size: 1870, SHA256: "8294c84c74f2e73d"},
		{Offset: 11153, Size: 727, SHA256: "930962420008a0cc"},
		{Offset: 11880, Size: 1205, SHA256: "a234c6dd2fea2456"},
		{Offset: 13085, Size: 1534, SHA256: "f55b2fb32d14d336"},
		{Offset: 14619, Size: 1314, SHA256: "c881dc0800dafc81"},
		{Offset: 15933, Size: 1936, SHA256: "c8cc6e0663883db0"},
		{Offset: 17869, Size: 580, SHA256: "8e00b3e570c9501f"},
		{Offset: 18449, Size: 755, SHA256: "8c0d38a3ef952f16"},
		{Offset: 19204, Size: 2076, SHA256: "8561c22abd60d788"},
		{Offset: 21280, Size: 1222, SHA256: "874952b1cfff3964"},
		{Offset: 22502, Size: 632, SHA256: "f451aee2d8573179"},
		{Offset: 23134, Size: 544, SHA256: "c119c33579d026d0"},
		{Offset: 23678, Size: 1307, SHA256: "67247f9ec03994c8"},
		{Offset: 24985, Size: 2170, SHA256: "2c5fc699c0db1322"},
		{Offset: 27155, Size: 971, SHA256: "a2ff1df325bdbefe"},
		{Offset: 28126, Size: 2426, SHA256: "c813d34b84313400"},
		{Offset: 30552, Size: 1071, SHA256: "d94fd9d8f07366c3"},
		{Offset: 31623, Size: 1129, SHA256: "786341a80f7c48ae"},
		{Offset: 32752, Size: 905, SHA256: "9fa7572bcfc61a4b"},
		{Offset: 33657, Size: 745, SHA256: "34fd94e7598f9923"},
		{Offset: 34402, Size: 880, SHA256: "195343c81d6c0aef"},
		{Offset: 35282, Size: 2054, SHA256: "ac6cdfd187cb0573"},
		{Offset: 37336, Size: 1476, SHA256: "41efce69f734a23f"},
		{Offset: 38812, Size: 3007, SHA256: "cf0f3bc05739ee77"},
		{Offset: 41819, Size: 556, SHA256: "4a97677994c70bfd"},
		{Offset: 42375, Size: 523, SHA256: "cd774898b23b1067"},
		{Offset: 42898, Size: 573, SHA256: "374d3cfd975c2596"},
		{Offset: 43471, Size: 4096, SHA256: "fd52afdb624b02c7"},
		{Offset: 47567, Size: 2885, SHA256: "fe4b1451a7087151"},
		{Offset: 50452, Size: 3114, SHA256: "459f147d71470bed"},
		{Offset: 53566, Size: 1171, SHA256: "2792359ae18ef8db"},
		{Offset: 54737, Size: 1421, SHA256: "04c76037ccb067b1"},
		{Offset: 56158, Size: 1758, SHA256: "8d9c4a1fed6b8239"},
		{Offset: 57916, Size: 1099, SHA256: "60a3a77dc8873d4e"},
		{Offset: 59015, Size: 1217, SHA256: "a04745bf6e13b083"},
		{Offset: 60232, Size: 1974, SHA256: "458350110297042e"},
		{Offset: 62206, Size: 1651, SHA256: "9a42aa17ef8c1065"},
		{Offset: 63857, Size: 916, SHA256: "4b40c070c5563627"},
		{Offset: 64773, Size: 763, SHA256: "0cbaf13ef5f7e644"},
	}
	if len(got) != len(want) {
		t.Fatalf("chunk count = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("chunk %d = %#v, want %#v", i, got[i], want[i])
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

type chunkSummary struct {
	Offset int64
	Size   int
	SHA256 string
}

func collectSummaries(t *testing.T, splitter *Splitter, data []byte) []chunkSummary {
	t.Helper()
	var summaries []chunkSummary
	err := splitter.Stream(context.Background(), bytes.NewReader(data), func(chunk Chunk) error {
		sum := sha256.Sum256(chunk.Data)
		summaries = append(summaries, chunkSummary{
			Offset: chunk.Offset,
			Size:   len(chunk.Data),
			SHA256: hex.EncodeToString(sum[:8]),
		})
		return nil
	})
	if err != nil {
		t.Fatalf("stream failed: %v", err)
	}
	return summaries
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
