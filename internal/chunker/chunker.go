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
	var chunks []Chunk
	err := s.Stream(ctx, reader, func(chunk Chunk) error {
		data := make([]byte, len(chunk.Data))
		copy(data, chunk.Data)
		chunks = append(chunks, Chunk{Offset: chunk.Offset, Data: data})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return chunks, nil
}

func (s *Splitter) Stream(ctx context.Context, reader io.Reader, emit func(Chunk) error) error {
	if emit == nil {
		return fmt.Errorf("emit callback must not be nil")
	}
	processor := streamProcessor{
		splitter: s,
		emit:     emit,
		current:  make([]byte, 0, s.maxSize),
	}

	if s.smallFileThreshold > 0 {
		prefix, exceeded, err := readSmallPrefix(ctx, reader, s.smallFileThreshold)
		if err != nil {
			return err
		}
		if !exceeded {
			return emit(Chunk{Offset: 0, Data: prefix})
		}
		if err := processor.write(ctx, prefix); err != nil {
			return err
		}
	}

	buffer := make([]byte, 32*1024)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := reader.Read(buffer)
		if n > 0 {
			if writeErr := processor.write(ctx, buffer[:n]); writeErr != nil {
				return writeErr
			}
		}
		if err == io.EOF {
			return processor.flush(ctx)
		}
		if err != nil {
			return fmt.Errorf("read stream: %w", err)
		}
	}
}

type streamProcessor struct {
	splitter *Splitter
	emit     func(Chunk) error
	offset   int64
	current  []byte
	hash     uint64
	emitted  bool
}

func (p *streamProcessor) write(ctx context.Context, data []byte) error {
	for _, b := range data {
		if err := ctx.Err(); err != nil {
			return err
		}
		p.current = append(p.current, b)
		p.hash = (p.hash << 1) + gearTable[b]
		size := len(p.current)
		if size < p.splitter.minSize {
			continue
		}
		if size >= p.splitter.maxSize || p.hash&p.splitter.mask == 0 {
			if err := p.emitCurrent(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *streamProcessor) flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(p.current) == 0 {
		if p.emitted {
			return nil
		}
		return p.emit(Chunk{Offset: 0, Data: nil})
	}
	return p.emitCurrent(ctx)
}

func (p *streamProcessor) emitCurrent(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	chunk := Chunk{Offset: p.offset, Data: p.current}
	if err := p.emit(chunk); err != nil {
		return err
	}
	p.offset += int64(len(p.current))
	p.current = make([]byte, 0, p.splitter.maxSize)
	p.hash = 0
	p.emitted = true
	return nil
}

func readSmallPrefix(ctx context.Context, reader io.Reader, threshold int) ([]byte, bool, error) {
	prefix := make([]byte, 0, minInt(threshold, 64*1024))
	buffer := make([]byte, 32*1024)
	for len(prefix) <= threshold {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		n, err := reader.Read(buffer)
		if n > 0 {
			prefix = append(prefix, buffer[:n]...)
			if len(prefix) > threshold {
				return prefix, true, nil
			}
		}
		if err == io.EOF {
			return prefix, false, nil
		}
		if err != nil {
			return nil, false, fmt.Errorf("read stream: %w", err)
		}
	}
	return prefix, true, nil
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

func minInt(a int, b int) int {
	if a < b {
		return a
	}
	return b
}
