package net

import (
	"sync"
	"time"
)

// SpeedMeter measures transfer speed (bytes/sec) using exponential moving average.
// Unlike a sliding window, EMA holds its value when no new data arrives —
// speed won't decay just because the producer is blocked on a full buffer.
type SpeedMeter struct {
	mu        sync.Mutex
	alpha     float64 // smoothing factor, 0 < alpha <= 1
	speed     float64 // current EMA speed (bytes/sec)
	lastBytes int64
	lastTime  time.Time
}

// NewSpeedMeter creates a SpeedMeter. alpha controls responsiveness:
// higher values react faster to changes but are noisier.
// window is kept for API compatibility with the old signature.
func NewSpeedMeter(window time.Duration, maxSamples int) *SpeedMeter {
	_ = maxSamples
	return &SpeedMeter{
		alpha: 0.3,
	}
}

// Record adds a data point: `bytes` were transferred at the current time.
func (sm *SpeedMeter) Record(bytes int64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	now := time.Now()

	// First sample: just record the starting point
	if sm.lastTime.IsZero() {
		sm.lastBytes = bytes
		sm.lastTime = now
		return
	}

	elapsed := now.Sub(sm.lastTime).Seconds()
	if elapsed <= 0 {
		sm.lastBytes += bytes
		return
	}

	instantRate := float64(sm.lastBytes) / elapsed

	if sm.speed == 0 {
		sm.speed = instantRate
	} else {
		sm.speed = sm.alpha*instantRate + (1-sm.alpha)*sm.speed
	}

	sm.lastBytes = bytes
	sm.lastTime = now
}

// Speed returns the current EMA speed in bytes/sec.
// When no data has arrived, returns the last known value (does not decay).
func (sm *SpeedMeter) Speed() float64 {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// If no data for > 5 seconds, decay slowly toward 0
	if !sm.lastTime.IsZero() && time.Since(sm.lastTime) > 5*time.Second && sm.speed > 0 {
		sm.speed *= 0.9
	}

	return sm.speed
}

// Reset clears all recorded state.
func (sm *SpeedMeter) Reset() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.speed = 0
	sm.lastBytes = 0
	sm.lastTime = time.Time{}
}
