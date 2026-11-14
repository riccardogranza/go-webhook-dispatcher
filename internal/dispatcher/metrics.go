package dispatcher

import (
	"fmt"
	"math"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const reservoirCapacity = 16384

// Metrics tracks atomic counters and a bounded latency ring buffer for quantile calculation.
type Metrics struct {
	enqueued  atomic.Uint64
	delivered atomic.Uint64
	retried   atomic.Uint64
	dlqCount  atomic.Int64
	dlqTotal  atomic.Uint64
	replayed  atomic.Uint64
	shedCount atomic.Uint64
	inFlight  atomic.Int64
	startedAt time.Time

	mu         sync.Mutex
	latencies  []float64 // stored in milliseconds
	writeIndex int
	full       bool
}

// MetricsSnapshot represents a point-in-time view of all engine telemetry.
type MetricsSnapshot struct {
	Enqueued      uint64  `json:"enqueued"`
	Delivered     uint64  `json:"delivered"`
	Retried       uint64  `json:"retried"`
	DLQCount      int64   `json:"dlq_count"`
	DLQTotal      uint64  `json:"dlq_total"`
	Replayed      uint64  `json:"replayed"`
	ShedCount     uint64  `json:"shed_count"`
	InFlight      int64   `json:"in_flight"`
	QueueDepth    int     `json:"queue_depth"`
	QueueCapacity int     `json:"queue_capacity"`
	P50LatencyMs  float64 `json:"p50_latency_ms"`
	P95LatencyMs  float64 `json:"p95_latency_ms"`
	P99LatencyMs  float64 `json:"p99_latency_ms"`
	PeakRSSBytes  int64   `json:"peak_rss_bytes"`
	PeakRSSMB     float64 `json:"peak_rss_mb"`
	HeapAllocMB   float64 `json:"heap_alloc_mb"`
	Goroutines    int     `json:"goroutines"`
	UptimeSeconds float64 `json:"uptime_seconds"`
}

// NewMetrics initializes a new telemetry collector.
func NewMetrics() *Metrics {
	return &Metrics{
		startedAt: time.Now(),
		latencies: make([]float64, 0, reservoirCapacity),
	}
}

func (m *Metrics) IncEnqueued()  { m.enqueued.Add(1) }
func (m *Metrics) IncDelivered() { m.delivered.Add(1) }
func (m *Metrics) IncRetried()   { m.retried.Add(1) }
func (m *Metrics) IncShed()      { m.shedCount.Add(1) }

func (m *Metrics) IncDLQ() {
	m.dlqCount.Add(1)
	m.dlqTotal.Add(1)
}

func (m *Metrics) DecDLQOnReplay() {
	m.dlqCount.Add(-1)
	m.replayed.Add(1)
}

func (m *Metrics) SetDLQCount(n int64) {
	m.dlqCount.Store(n)
}

func (m *Metrics) IncInFlight() { m.inFlight.Add(1) }
func (m *Metrics) DecInFlight() { m.inFlight.Add(-1) }

// RecordDeliveryLatency records a webhook delivery duration in the ring buffer.
func (m *Metrics) RecordDeliveryLatency(d time.Duration) {
	ms := float64(d.Microseconds()) / 1000.0
	m.mu.Lock()
	if len(m.latencies) < reservoirCapacity {
		m.latencies = append(m.latencies, ms)
	} else {
		m.latencies[m.writeIndex] = ms
		m.full = true
	}
	m.writeIndex = (m.writeIndex + 1) % reservoirCapacity
	m.mu.Unlock()
}

// Snapshot computes current counters, latency percentiles, and OS process memory telemetry.
func (m *Metrics) Snapshot(queueDepth, queueCap int) MetricsSnapshot {
	m.mu.Lock()
	sampleCount := len(m.latencies)
	samples := make([]float64, sampleCount)
	copy(samples, m.latencies)
	m.mu.Unlock()

	var p50, p95, p99 float64
	if sampleCount > 0 {
		slices.Sort(samples)
		p50 = roundMs(percentileSorted(samples, 0.50))
		p95 = roundMs(percentileSorted(samples, 0.95))
		p99 = roundMs(percentileSorted(samples, 0.99))
	}

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	rssBytes := ReadPeakRSSBytes()

	dlq := m.dlqCount.Load()
	if dlq < 0 {
		dlq = 0
	}

	return MetricsSnapshot{
		Enqueued:      m.enqueued.Load(),
		Delivered:     m.delivered.Load(),
		Retried:       m.retried.Load(),
		DLQCount:      dlq,
		DLQTotal:      m.dlqTotal.Load(),
		Replayed:      m.replayed.Load(),
		ShedCount:     m.shedCount.Load(),
		InFlight:      m.inFlight.Load(),
		QueueDepth:    queueDepth,
		QueueCapacity: queueCap,
		P50LatencyMs:  p50,
		P95LatencyMs:  p95,
		P99LatencyMs:  p99,
		PeakRSSBytes:  rssBytes,
		PeakRSSMB:     roundMs(float64(rssBytes) / (1024.0 * 1024.0)),
		HeapAllocMB:   roundMs(float64(mem.HeapAlloc) / (1024.0 * 1024.0)),
		Goroutines:    runtime.NumGoroutine(),
		UptimeSeconds: roundMs(time.Since(m.startedAt).Seconds()),
	}
}

// FormatPrometheus renders the snapshot in standard Prometheus text exposition format.
func (s MetricsSnapshot) FormatPrometheus() string {
	var b strings.Builder
	b.WriteString("# HELP webhook_dispatcher_enqueued_total Total number of events accepted and persisted.\n")
	b.WriteString("# TYPE webhook_dispatcher_enqueued_total counter\n")
	fmt.Fprintf(&b, "webhook_dispatcher_enqueued_total %d\n", s.Enqueued)

	b.WriteString("# HELP webhook_dispatcher_delivered_total Total number of webhooks successfully delivered.\n")
	b.WriteString("# TYPE webhook_dispatcher_delivered_total counter\n")
	fmt.Fprintf(&b, "webhook_dispatcher_delivered_total %d\n", s.Delivered)

	b.WriteString("# HELP webhook_dispatcher_retried_total Total number of delivery retry attempts.\n")
	b.WriteString("# TYPE webhook_dispatcher_retried_total counter\n")
	fmt.Fprintf(&b, "webhook_dispatcher_retried_total %d\n", s.Retried)

	b.WriteString("# HELP webhook_dispatcher_dlq_count Current number of events in the Dead Letter Queue.\n")
	b.WriteString("# TYPE webhook_dispatcher_dlq_count gauge\n")
	fmt.Fprintf(&b, "webhook_dispatcher_dlq_count %d\n", s.DLQCount)

	b.WriteString("# HELP webhook_dispatcher_shed_count_total Total number of requests shed via HTTP 429 backpressure.\n")
	b.WriteString("# TYPE webhook_dispatcher_shed_count_total counter\n")
	fmt.Fprintf(&b, "webhook_dispatcher_shed_count_total %d\n", s.ShedCount)

	b.WriteString("# HELP webhook_dispatcher_delivery_latency_ms Webhook delivery latency percentiles in milliseconds.\n")
	b.WriteString("# TYPE webhook_dispatcher_delivery_latency_ms summary\n")
	fmt.Fprintf(&b, "webhook_dispatcher_delivery_latency_ms{quantile=\"0.5\"} %.3f\n", s.P50LatencyMs)
	fmt.Fprintf(&b, "webhook_dispatcher_delivery_latency_ms{quantile=\"0.95\"} %.3f\n", s.P95LatencyMs)
	fmt.Fprintf(&b, "webhook_dispatcher_delivery_latency_ms{quantile=\"0.99\"} %.3f\n", s.P99LatencyMs)

	b.WriteString("# HELP webhook_dispatcher_peak_rss_bytes Peak resident set size in bytes.\n")
	b.WriteString("# TYPE webhook_dispatcher_peak_rss_bytes gauge\n")
	fmt.Fprintf(&b, "webhook_dispatcher_peak_rss_bytes %d\n", s.PeakRSSBytes)
	return b.String()
}

func percentileSorted(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[len(sorted)-1]
	}
	idx := p * float64(len(sorted)-1)
	lower := int(math.Floor(idx))
	upper := int(math.Ceil(idx))
	if lower == upper || upper >= len(sorted) {
		return sorted[lower]
	}
	weight := idx - float64(lower)
	return sorted[lower]*(1-weight) + sorted[upper]*weight
}

func roundMs(v float64) float64 {
	return math.Round(v*1000) / 1000
}

// ReadPeakRSSBytes returns the peak resident set size (RSS) of the current process in bytes.
// On Darwin (macOS), Maxrss from getrusage is reported in bytes; on Linux it is in kilobytes.
func ReadPeakRSSBytes() int64 {
	var rusage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &rusage); err == nil && rusage.Maxrss > 0 {
		if runtime.GOOS == "darwin" {
			return int64(rusage.Maxrss)
		}
		return int64(rusage.Maxrss) * 1024
	}
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	return int64(mem.Sys)
}
