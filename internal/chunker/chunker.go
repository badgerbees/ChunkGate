package chunker

import (
	"context"
	"fmt"
	"io"
)

const (
	DefaultMinSize            = 512 * 1024
	DefaultAvgSize            = 1024 * 1024
	DefaultMaxSize            = 4 * 1024 * 1024
	DefaultSmallFileThreshold = 5 * 1024 * 1024
)

type Options struct {
	MinSize            int
	AvgSize            int
	MaxSize            int
	SmallFileThreshold int
}

type Chunk struct {
	Offset int64
	Data   []byte
}

type Splitter struct {
	minSize            int
	avgSize            int
	maxSize            int
	smallFileThreshold int
	mask               uint64
}

var gearTable = buildGearTable()

func New(options Options) *Splitter {
	if options.MinSize <= 0 {
		options.MinSize = DefaultMinSize
	}
	if options.AvgSize <= 0 {
		options.AvgSize = DefaultAvgSize
	}
	if options.MaxSize <= 0 {
		options.MaxSize = DefaultMaxSize
	}
	if options.SmallFileThreshold < 0 {
		options.SmallFileThreshold = DefaultSmallFileThreshold
	}
	if options.MinSize > options.AvgSize {
		options.AvgSize = options.MinSize
	}
	if options.AvgSize > options.MaxSize {
		options.MaxSize = options.AvgSize
	}
	return &Splitter{
		minSize:            options.MinSize,
		avgSize:            options.AvgSize,
		maxSize:            options.MaxSize,
		smallFileThreshold: options.SmallFileThreshold,
		mask:               uint64(nextPowerOfTwo(options.AvgSize) - 1),
	}
}

func (s *Splitter) Split(ctx context.Context, reader io.Reader) ([]Chunk, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read stream: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return []Chunk{{Offset: 0, Data: nil}}, nil
	}
	if s.smallFileThreshold > 0 && len(data) <= s.smallFileThreshold {
		return []Chunk{{Offset: 0, Data: data}}, nil
	}

	chunks := make([]Chunk, 0, (len(data)/s.avgSize)+1)
	for offset := 0; offset < len(data); {
		end := s.boundary(data, offset)
		chunk := make([]byte, end-offset)
		copy(chunk, data[offset:end])
		chunks = append(chunks, Chunk{Offset: int64(offset), Data: chunk})
		offset = end
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	return chunks, nil
}

func (s *Splitter) boundary(data []byte, start int) int {
	remaining := len(data) - start
	if remaining <= s.maxSize {
		if remaining <= s.minSize {
			return len(data)
		}
	}

	minEnd := start + s.minSize
	if minEnd > len(data) {
		return len(data)
	}
	maxEnd := start + s.maxSize
	if maxEnd > len(data) {
		maxEnd = len(data)
	}

	var hash uint64
	for i := start; i < maxEnd; i++ {
		hash = (hash << 1) + gearTable[data[i]]
		if i+1 < minEnd {
			continue
		}
		if hash&s.mask == 0 {
			return i + 1
		}
	}
	return maxEnd
}

func nextPowerOfTwo(n int) int {
	if n <= 1 {
		return 1
	}
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

func buildGearTable() [256]uint64 {
	var table [256]uint64
	seed := uint64(0x9e3779b97f4a7c15)
	for i := range table {
		seed += 0x9e3779b97f4a7c15
		z := seed
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		table[i] = z ^ (z >> 31)
	}
	return table
}
