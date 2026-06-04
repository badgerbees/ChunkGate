package chunker

import (
	"bytes"
	"context"
	"testing"
)

var benchData = makeBenchmarkData(16 * 1024 * 1024)

func BenchmarkStreamFastCDC(b *testing.B) {
	benchmarkStream(b, EngineFastCDC)
}

func BenchmarkStreamBuiltin(b *testing.B) {
	benchmarkStream(b, EngineBuiltin)
}

func benchmarkStream(b *testing.B, engine string) {
	splitter := New(Options{
		MinSize:            512 * 1024,
		AvgSize:            1024 * 1024,
		MaxSize:            4 * 1024 * 1024,
		SmallFileThreshold: 0,
		Engine:             engine,
	})
	b.SetBytes(int64(len(benchData)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var chunks int
		var total int64
		err := splitter.Stream(context.Background(), bytes.NewReader(benchData), func(chunk Chunk) error {
			chunks++
			total += int64(len(chunk.Data))
			return nil
		})
		if err != nil {
			b.Fatalf("stream failed: %v", err)
		}
		if total != int64(len(benchData)) {
			b.Fatalf("total = %d, want %d", total, len(benchData))
		}
		if i == 0 && chunks > 0 {
			b.ReportMetric(float64(total)/float64(chunks), "avg_chunk_B")
			b.ReportMetric(float64(chunks), "chunks/op")
		}
	}
}

func makeBenchmarkData(size int) []byte {
	data := make([]byte, size)
	var x uint64 = 0x123456789abcdef0
	for i := range data {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		data[i] = byte(x)
	}
	return data
}
