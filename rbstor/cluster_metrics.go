package rbstor

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CIDgravity/filecoin-gateway/iface"
)

// ClusterMetrics collects and stores metrics for cluster monitoring.
//
// Design: hot-path recording (RecordRead, RecordWrite, StartRead, etc.) uses
// atomic counters + a small latency-buffer mutex so it never contends with
// readers.  A background ticker calls collect() every 10s under a write lock.
// All read-side methods (Get*) only take a read lock and never write-lock,
// eliminating the Lock/Unlock/RLock convoy that previously caused goroutine
// pileup.
type ClusterMetrics struct {
	// --- hot-path atomics (no lock needed) ---
	totalReads      atomic.Int64
	totalWrites     atomic.Int64
	totalErrors     atomic.Int64
	activeReads     atomic.Int64
	activeWrites    atomic.Int64
	activeMultipart atomic.Int64
	totalReadBytes  atomic.Int64
	totalWriteBytes atomic.Int64

	// Per-interval counters (swapped atomically at collect)
	intervalReads      atomic.Int64
	intervalWrites     atomic.Int64
	intervalErrors     atomic.Int64
	intervalReadBytes  atomic.Int64
	intervalWriteBytes atomic.Int64

	// Latency buffer needs a separate small lock because append is not atomic
	latMu                  sync.Mutex
	intervalReadLatencies  []float64
	intervalWriteLatencies []float64

	// --- time-series data, protected by mu ---
	mu             sync.RWMutex
	timestamps     []int64
	readCounts     []int64
	writeCounts    []int64
	errorCounts    []int64
	readLatencies  [][]float64 // each entry is latencies for that interval
	writeLatencies [][]float64
	readBytes      []int64
	writeBytes     []int64

	// Events
	events []iface.ClusterEvent

	startTime time.Time

	stopOnce sync.Once
	stopCh   chan struct{}
}

const (
	maxDataPoints   = 60 // 10 minutes of data
	collectInterval = 10 * time.Second
	maxEvents       = 100
)

var globalClusterMetrics *ClusterMetrics

func init() {
	m := &ClusterMetrics{
		startTime: time.Now(),
		stopCh:    make(chan struct{}),
	}
	globalClusterMetrics = m
	go m.collectLoop()
}

// collectLoop runs the background collection ticker
func (m *ClusterMetrics) collectLoop() {
	ticker := time.NewTicker(collectInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.collect()
		case <-m.stopCh:
			return
		}
	}
}

// Stop stops the background collection loop (for testing)
func (m *ClusterMetrics) Stop() {
	m.stopOnce.Do(func() { close(m.stopCh) })
}

// GetClusterMetrics returns the global cluster metrics instance
func GetClusterMetrics() *ClusterMetrics {
	return globalClusterMetrics
}

// RecordRead records a read operation with bytes transferred
func (m *ClusterMetrics) RecordRead(latencyMs float64, bytes int64, err error) {
	m.totalReads.Add(1)
	m.intervalReads.Add(1)
	m.totalReadBytes.Add(bytes)
	m.intervalReadBytes.Add(bytes)
	if err != nil {
		m.totalErrors.Add(1)
		m.intervalErrors.Add(1)
	}

	m.latMu.Lock()
	m.intervalReadLatencies = append(m.intervalReadLatencies, latencyMs)
	m.latMu.Unlock()
}

// RecordWrite records a write operation with bytes transferred
func (m *ClusterMetrics) RecordWrite(latencyMs float64, bytes int64, err error) {
	m.totalWrites.Add(1)
	m.intervalWrites.Add(1)
	m.totalWriteBytes.Add(bytes)
	m.intervalWriteBytes.Add(bytes)
	if err != nil {
		m.totalErrors.Add(1)
		m.intervalErrors.Add(1)
	}

	m.latMu.Lock()
	m.intervalWriteLatencies = append(m.intervalWriteLatencies, latencyMs)
	m.latMu.Unlock()
}

// StartRead marks a read as in-flight
func (m *ClusterMetrics) StartRead() {
	m.activeReads.Add(1)
}

// EndRead marks a read as complete
func (m *ClusterMetrics) EndRead() {
	m.activeReads.Add(-1)
}

// StartWrite marks a write as in-flight
func (m *ClusterMetrics) StartWrite() {
	m.activeWrites.Add(1)
}

// EndWrite marks a write as complete
func (m *ClusterMetrics) EndWrite() {
	m.activeWrites.Add(-1)
}

// AddEvent adds a cluster event
func (m *ClusterMetrics) AddEvent(eventType, message, nodeID string) {
	event := iface.ClusterEvent{
		Timestamp: time.Now().Unix(),
		Type:      eventType,
		Message:   message,
		NodeID:    nodeID,
	}

	m.mu.Lock()
	m.events = append([]iface.ClusterEvent{event}, m.events...)
	if len(m.events) > maxEvents {
		m.events = m.events[:maxEvents]
	}
	m.mu.Unlock()
}

// collect swaps interval counters and appends to time series.
// Called by the background ticker under write lock.
func (m *ClusterMetrics) collect() {
	now := time.Now()

	// Atomically swap interval counters to zero and capture their values
	reads := m.intervalReads.Swap(0)
	writes := m.intervalWrites.Swap(0)
	errors := m.intervalErrors.Swap(0)
	readB := m.intervalReadBytes.Swap(0)
	writeB := m.intervalWriteBytes.Swap(0)

	// Swap latency slices under the small latency lock
	m.latMu.Lock()
	readLat := m.intervalReadLatencies
	writeLat := m.intervalWriteLatencies
	m.intervalReadLatencies = nil
	m.intervalWriteLatencies = nil
	m.latMu.Unlock()

	// Now take the write lock to append to time series
	m.mu.Lock()
	defer m.mu.Unlock()

	m.timestamps = append(m.timestamps, now.Unix())
	m.readCounts = append(m.readCounts, reads)
	m.writeCounts = append(m.writeCounts, writes)
	m.errorCounts = append(m.errorCounts, errors)
	m.readLatencies = append(m.readLatencies, readLat)
	m.writeLatencies = append(m.writeLatencies, writeLat)
	m.readBytes = append(m.readBytes, readB)
	m.writeBytes = append(m.writeBytes, writeB)

	// Trim to max data points
	if len(m.timestamps) > maxDataPoints {
		m.timestamps = m.timestamps[1:]
		m.readCounts = m.readCounts[1:]
		m.writeCounts = m.writeCounts[1:]
		m.errorCounts = m.errorCounts[1:]
		m.readLatencies = m.readLatencies[1:]
		m.writeLatencies = m.writeLatencies[1:]
		m.readBytes = m.readBytes[1:]
		m.writeBytes = m.writeBytes[1:]
	}
}

// GetThroughputHistory returns historical throughput data
func (m *ClusterMetrics) GetThroughputHistory(duration string) iface.ThroughputHistory {
	m.mu.RLock()
	defer m.mu.RUnlock()

	points := len(m.timestamps)
	if points == 0 {
		now := time.Now().Unix()
		curReads := m.intervalReads.Load()
		curWrites := m.intervalWrites.Load()
		return iface.ThroughputHistory{
			Timestamps: []int64{now},
			Total:      []float64{float64(curReads+curWrites) / collectInterval.Seconds()},
			Reads:      []float64{float64(curReads) / collectInterval.Seconds()},
			Writes:     []float64{float64(curWrites) / collectInterval.Seconds()},
			ByProxy:    make(map[string][]float64),
		}
	}

	timestamps := make([]int64, points)
	total := make([]float64, points)
	reads := make([]float64, points)
	writes := make([]float64, points)

	for i := 0; i < points; i++ {
		timestamps[i] = m.timestamps[i]
		reads[i] = float64(m.readCounts[i]) / collectInterval.Seconds()
		writes[i] = float64(m.writeCounts[i]) / collectInterval.Seconds()
		total[i] = reads[i] + writes[i]
	}

	return iface.ThroughputHistory{
		Timestamps: timestamps,
		Total:      total,
		Reads:      reads,
		Writes:     writes,
		ByProxy:    make(map[string][]float64),
	}
}

// GetIOThroughputHistory returns historical I/O bytes throughput data
func (m *ClusterMetrics) GetIOThroughputHistory(duration string) iface.IOThroughputHistory {
	m.mu.RLock()
	defer m.mu.RUnlock()

	points := len(m.timestamps)
	if points == 0 {
		now := time.Now().Unix()
		curReadB := m.intervalReadBytes.Load()
		curWriteB := m.intervalWriteBytes.Load()
		return iface.IOThroughputHistory{
			Timestamps: []int64{now},
			ReadBytes:  []float64{float64(curReadB) / collectInterval.Seconds()},
			WriteBytes: []float64{float64(curWriteB) / collectInterval.Seconds()},
			TotalBytes: []float64{float64(curReadB+curWriteB) / collectInterval.Seconds()},
		}
	}

	timestamps := make([]int64, points)
	readBytes := make([]float64, points)
	writeBytes := make([]float64, points)
	totalBytes := make([]float64, points)

	for i := 0; i < points; i++ {
		timestamps[i] = m.timestamps[i]
		readBytes[i] = float64(m.readBytes[i]) / collectInterval.Seconds()
		writeBytes[i] = float64(m.writeBytes[i]) / collectInterval.Seconds()
		totalBytes[i] = readBytes[i] + writeBytes[i]
	}

	return iface.IOThroughputHistory{
		Timestamps: timestamps,
		ReadBytes:  readBytes,
		WriteBytes: writeBytes,
		TotalBytes: totalBytes,
	}
}

// percentile calculates the p-th percentile of a sorted slice
func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

// GetLatencyDistribution returns latency percentiles
func (m *ClusterMetrics) GetLatencyDistribution(duration string) iface.LatencyDistribution {
	m.mu.RLock()
	defer m.mu.RUnlock()

	points := len(m.timestamps)
	if points == 0 {
		return iface.LatencyDistribution{
			Timestamps: []int64{time.Now().Unix()},
			P50:        []float64{0},
			P95:        []float64{0},
			P99:        []float64{0},
			ByOperation: make(map[string]struct {
				P50 []float64 `json:"p50"`
				P95 []float64 `json:"p95"`
				P99 []float64 `json:"p99"`
			}),
		}
	}

	timestamps := make([]int64, points)
	p50 := make([]float64, points)
	p95 := make([]float64, points)
	p99 := make([]float64, points)

	for i := 0; i < points; i++ {
		timestamps[i] = m.timestamps[i]

		// Combine read and write latencies for this interval
		all := append([]float64{}, m.readLatencies[i]...)
		all = append(all, m.writeLatencies[i]...)

		if len(all) > 0 {
			sort.Float64s(all)
			p50[i] = percentile(all, 0.50)
			p95[i] = percentile(all, 0.95)
			p99[i] = percentile(all, 0.99)
		}
	}

	return iface.LatencyDistribution{
		Timestamps: timestamps,
		P50:        p50,
		P95:        p95,
		P99:        p99,
		ByOperation: make(map[string]struct {
			P50 []float64 `json:"p50"`
			P95 []float64 `json:"p95"`
			P99 []float64 `json:"p99"`
		}),
	}
}

// GetErrorRates returns error statistics
func (m *ClusterMetrics) GetErrorRates() iface.ErrorRates {
	totalOps := m.totalReads.Load() + m.totalWrites.Load()
	totalErrs := m.totalErrors.Load()
	errorRate := float64(0)
	if totalOps > 0 {
		errorRate = float64(totalErrs) / float64(totalOps) * 100
	}

	trend := "stable"
	if errorRate > 5 {
		trend = "degrading"
	} else if errorRate < 1 && totalOps > 100 {
		trend = "improving"
	}

	return iface.ErrorRates{
		Nodes: map[string]iface.NodeErrorStats{
			"local": {
				ErrorRate: errorRate,
				ByType:    make(map[string]int),
				Trend:     trend,
			},
		},
	}
}

// GetActiveRequests returns current in-flight requests
func (m *ClusterMetrics) GetActiveRequests() iface.ActiveRequests {
	reads := m.activeReads.Load()
	writes := m.activeWrites.Load()
	multipart := m.activeMultipart.Load()

	return iface.ActiveRequests{
		Total:     int(reads + writes + multipart),
		Reads:     int(reads),
		Writes:    int(writes),
		Multipart: int(multipart),
		ByProxy:   make(map[string]int),
		ByStorage: make(map[string]int),
	}
}

// GetClusterEvents returns recent cluster events
func (m *ClusterMetrics) GetClusterEvents(limit int) []iface.ClusterEvent {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if limit <= 0 || limit > len(m.events) {
		limit = len(m.events)
	}

	result := make([]iface.ClusterEvent, limit)
	copy(result, m.events[:limit])
	return result
}
