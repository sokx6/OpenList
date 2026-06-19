package net

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

const (
	DefaultReadAheadChunkSize  = 256 * 1024
	DefaultReadAheadBufferSize = 64 * 1024 * 1024
	DefaultSpeedRatio          = 0.5
	speedUpdateInterval        = 500 * time.Millisecond
	speedWindow                = 3 * time.Second
	minUpstreamBytesForLimit   = 128 * 1024
	limiterActivationDelay     = 1000 * time.Millisecond
)

type ReadAheadConfig struct {
	ChunkSize  int
	BufferSize int
	SpeedRatio float64
	Enabled    bool
}

func DefaultReadAheadConfig() ReadAheadConfig {
	return ReadAheadConfig{
		ChunkSize:  DefaultReadAheadChunkSize,
		BufferSize: DefaultReadAheadBufferSize,
		SpeedRatio: DefaultSpeedRatio,
		Enabled:    false,
	}
}

type readAheadChunk struct {
	data []byte
	n    int
}

type ReadAheadProxyReader struct {
	ctx    context.Context
	cancel context.CancelFunc

	upstream      io.ReadCloser
	upstreamMeter *SpeedMeter
	limiter       *rate.Limiter

	chunks     chan *readAheadChunk
	chunkCap   int
	current    *readAheadChunk
	currentOff int

	speedRatio    float64
	upstreamBytes atomic.Int64
	limiterActive atomic.Bool

	bgErr  error
	bgDone chan struct{}
	bgOnce sync.Once
}

func NewReadAheadProxyReader(ctx context.Context, upstream io.ReadCloser, cfg ReadAheadConfig) *ReadAheadProxyReader {
	if !cfg.Enabled {
		return nil
	}

	chunkSize := cfg.ChunkSize
	if chunkSize <= 0 {
		chunkSize = DefaultReadAheadChunkSize
	}
	bufferSize := cfg.BufferSize
	if bufferSize <= 0 {
		bufferSize = DefaultReadAheadBufferSize
	}
	speedRatio := cfg.SpeedRatio
	if speedRatio <= 0 || speedRatio > 1 {
		speedRatio = DefaultSpeedRatio
	}

	numChunks := bufferSize / chunkSize
	if numChunks < 2 {
		numChunks = 2
	}

	ctx, cancel := context.WithCancel(ctx)

	r := &ReadAheadProxyReader{
		ctx:           ctx,
		cancel:        cancel,
		upstream:      upstream,
		upstreamMeter: NewSpeedMeter(speedWindow, 0),
		limiter:       rate.NewLimiter(rate.Inf, 0),
		chunks:        make(chan *readAheadChunk, numChunks),
		chunkCap:      numChunks,
		speedRatio:    speedRatio,
		bgDone:        make(chan struct{}),
	}

	go r.backgroundDownload(chunkSize)
	go r.speedUpdater()

	return r
}

func (r *ReadAheadProxyReader) backgroundDownload(chunkSize int) {
	defer close(r.chunks)
	defer r.bgOnce.Do(func() { close(r.bgDone) })

	buf := make([]byte, chunkSize)
	for {
		n, err := r.upstream.Read(buf)
		if n > 0 {
			r.upstreamMeter.Record(int64(n))

			total := r.upstreamBytes.Add(int64(n))
			if !r.limiterActive.Load() && total >= minUpstreamBytesForLimit {
				r.limiterActive.Store(true)
			}

			chunk := &readAheadChunk{data: make([]byte, n)}
			copy(chunk.data, buf[:n])
			chunk.n = n

			select {
			case r.chunks <- chunk:
			case <-r.ctx.Done():
				r.bgErr = r.ctx.Err()
				return
			}
		}
		if err != nil {
			if err != io.EOF {
				r.bgErr = err
			}
			return
		}
	}
}

func (r *ReadAheadProxyReader) speedUpdater() {
	ticker := time.NewTicker(speedUpdateInterval)
	defer ticker.Stop()

	startTime := time.Now()

	for {
		select {
		case <-ticker.C:
			if !r.limiterActive.Load() && time.Since(startTime) > limiterActivationDelay {
				r.limiterActive.Store(true)
			}

			upstreamSpeed := r.upstreamMeter.Speed()
			queueLen := len(r.chunks)
			fillRatio := float64(queueLen) / float64(r.chunkCap)

			if upstreamSpeed > 0 {
				baseRate := upstreamSpeed * r.speedRatio

				switch {
				case fillRatio < 0.2:
					baseRate *= 0.3 + fillRatio
				case fillRatio > 0.8:
				default:
					baseRate *= 0.3 + fillRatio*0.7
				}

				burst := int(baseRate)
				if burst < 64*1024 {
					burst = 64 * 1024
				}
				if burst > int(upstreamSpeed*2) {
					burst = int(upstreamSpeed * 2)
				}
				r.limiter.SetLimit(rate.Limit(baseRate))
				r.limiter.SetBurst(burst)
			} else if r.upstreamBytes.Load() > 0 && fillRatio < 0.1 {
				current := float64(r.limiter.Limit())
				r.limiter.SetLimit(rate.Limit(current / 2))
			}
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *ReadAheadProxyReader) Read(p []byte) (n int, err error) {
	if r.current == nil || r.currentOff >= r.current.n {
		select {
		case chunk, ok := <-r.chunks:
			if !ok {
				return 0, r.wrapErr(io.EOF)
			}
			r.current = chunk
			r.currentOff = 0
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		}
	}

	if r.limiterActive.Load() {
		available := r.current.n - r.currentOff
		if available > len(p) {
			available = len(p)
		}
		if available > 0 {
			if err := r.limiter.WaitN(r.ctx, available); err != nil {
				return 0, err
			}
		}
	}

	n = copy(p, r.current.data[r.currentOff:r.current.n])
	r.currentOff += n
	return n, nil
}

func (r *ReadAheadProxyReader) Close() error {
	if r.upstream != nil {
		r.upstream.Close()
	}
	r.cancel()
	// Drain until the background goroutine observes the cancel and closes the
	// channel. A receive from a closed channel returns ok=false, ending the
	// range loop. The previous `select { case <-r.chunks: default: }` form
	// busy-spun forever once the channel was closed, because a closed-channel
	// receive is always ready and `default` was never selected — pinning a CPU
	// core per completed proxy request.
	for range r.chunks {
	}
	return nil
}

func (r *ReadAheadProxyReader) wrapErr(err error) error {
	if err == io.EOF && r.bgErr != nil {
		return fmt.Errorf("readahead proxy: upstream error: %w", r.bgErr)
	}
	return err
}
var ReadAheadProxyEnabled bool
var ReadAheadProxyBufferMB int
var ReadAheadProxySpeedRatio float64

func LoadReadAheadConfig() ReadAheadConfig {
	return ReadAheadConfig{
		ChunkSize:  DefaultReadAheadChunkSize,
		BufferSize: ReadAheadProxyBufferMB * 1024 * 1024,
		SpeedRatio: ReadAheadProxySpeedRatio,
		Enabled:    ReadAheadProxyEnabled,
	}
}
