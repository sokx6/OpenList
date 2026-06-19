package net

import (
	"context"
	"io"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// ReadAheadUploadReader wraps a user upload data source (io.ReadCloser)
// with a pre-fetch buffer and adaptive rate limiting.
//
// For uploads, the constraint is:
//
//	user_read_speed ≤ cloud_write_speed × SpeedRatio
//
// Architecture:
//
//	user data (original reader)
//	  → [producer goroutine: Read() + rate-limit]
//	    → buffered channel (pre-fetch buffer)
//	      → [consumer Read(): measure speed, feed limiter]
//	        → driver.Put() → cloud drive
//
// The consumer measures how fast the driver (cloud) is pulling data,
// and feeds this back to rate-limit the producer reading from the user.
type ReadAheadUploadReader struct {
	ctx    context.Context
	cancel context.CancelFunc

	origReader   io.ReadCloser // original user data source
	consumeMeter *SpeedMeter   // measures cloud-write speed proxy
	limiter      *rate.Limiter // controls user-read speed

	chunks     chan *readAheadChunk
	current    *readAheadChunk
	currentOff int

	speedRatio    float64
	producerBytes atomic.Int64
	limiterActive atomic.Bool
}

// NewReadAheadUploadReader creates a ReadAheadUploadReader.
// It takes ownership of origReader and will close it on Close().
// The returned reader should be passed to the driver's Put() as the data source.
func NewReadAheadUploadReader(ctx context.Context, origReader io.ReadCloser, cfg ReadAheadConfig) io.ReadCloser {
	if !cfg.Enabled || origReader == nil {
		return origReader
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

	r := &ReadAheadUploadReader{
		ctx:          ctx,
		cancel:       cancel,
		origReader:   origReader,
		consumeMeter: NewSpeedMeter(speedWindow, 128),
		limiter:      rate.NewLimiter(rate.Inf, 0),
		chunks:       make(chan *readAheadChunk, numChunks),
		speedRatio:   speedRatio,
	}

	go r.producer(chunkSize)
	go r.speedUpdater()

	return r
}

// producer reads from the user's original data source (rate-limited)
// and pushes chunks into the buffered channel.
func (r *ReadAheadUploadReader) producer(chunkSize int) {
	defer close(r.chunks)
	defer r.origReader.Close()

	buf := make([]byte, chunkSize)
	for {
		// Rate-limit: wait until the consumer has had time to drain
		if r.limiterActive.Load() {
			if err := r.limiter.WaitN(r.ctx, chunkSize); err != nil {
				return
			}
		}

		n, err := r.origReader.Read(buf)
		if n > 0 {
			r.producerBytes.Add(int64(n))

			chunk := &readAheadChunk{data: make([]byte, n)}
			copy(chunk.data, buf[:n])
			chunk.n = n

			select {
			case r.chunks <- chunk:
			case <-r.ctx.Done():
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// speedUpdater periodically recalibrates the rate limiter based on
// how fast the consumer (driver.Put() → cloud) is draining data.
func (r *ReadAheadUploadReader) speedUpdater() {
	ticker := time.NewTicker(speedUpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			consumeSpeed := r.consumeMeter.Speed()
			if consumeSpeed > 0 && r.producerBytes.Load() >= minUpstreamBytesForLimit {
				r.limiterActive.Store(true)
				targetRate := rate.Limit(consumeSpeed * r.speedRatio)
				burst := int(targetRate)
				if burst < 64*1024 {
					burst = 64 * 1024
				}
				r.limiter.SetLimit(targetRate)
				r.limiter.SetBurst(burst)
			}
		case <-r.ctx.Done():
			return
		}
	}
}

// Read implements io.Reader. Called by the driver's Put() to get upload data.
// It measures consumption speed and feeds back to rate-limit the producer.
func (r *ReadAheadUploadReader) Read(p []byte) (n int, err error) {
	if r.current == nil || r.currentOff >= r.current.n {
		select {
		case chunk, ok := <-r.chunks:
			if !ok {
				return 0, io.EOF
			}
			r.current = chunk
			r.currentOff = 0
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		}
	}

	n = copy(p, r.current.data[r.currentOff:r.current.n])
	r.currentOff += n
	if n > 0 {
		r.consumeMeter.Record(int64(n))
	}
	return n, nil
}

// Close releases resources. Cancels the producer goroutine and closes
// the original reader if not yet closed.
// Only cancels, does not drain r.chunks. The producer's Read() is not
// ctx-aware; if it's stuck on a hung client connection, the channel may
// never close, and draining would block Close forever, leaking this
// goroutine. The producer's defer close(r.chunks)/origReader.Close()
// handles cleanup; unread buffers are collected by GC.
func (r *ReadAheadUploadReader) Close() error {
	r.cancel()
	return nil
}
