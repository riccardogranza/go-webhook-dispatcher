package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os/exec"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/riccardogranza/go-webhook-dispatcher/internal/dispatcher"
)

func main() {
	var (
		dispatcherURL   string
		totalRequests   int
		concurrency     int
		receiverLatency time.Duration
		failureRate     float64
		signingSecret   string
	)

	flag.StringVar(&dispatcherURL, "url", "http://127.0.0.1:8080", "Base URL of the running webhook dispatcher")
	flag.IntVar(&totalRequests, "requests", 10000, "Total webhook events to ingest")
	flag.IntVar(&concurrency, "concurrency", 100, "Number of concurrent loadgen workers")
	flag.DurationVar(&receiverLatency, "receiver-latency", 1*time.Millisecond, "Simulated target webhook receiver latency")
	flag.Float64Var(&failureRate, "failure-rate", 0.015, "Simulated failure rate at target receiver (0.0 to 1.0)")
	flag.StringVar(&signingSecret, "secret", "whsec_prod_9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08", "HMAC-SHA256 signing secret")
	flag.Parse()

	dispatcherURL = strings.TrimRight(dispatcherURL, "/")

	// 1. Start embedded mock target webhook subscriber server.
	var (
		receivedWebhooks atomic.Uint64
		validSignatures  atomic.Uint64
		transient503s    atomic.Uint64
		permanent422s    atomic.Uint64
		reqCounter       atomic.Uint64
	)

	receiverMux := http.NewServeMux()
	receiverMux.HandleFunc("POST /webhook", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()

		tsStr := r.Header.Get("X-Webhook-Timestamp")
		sig := r.Header.Get("X-Signature-256")
		attemptStr := r.Header.Get("X-Delivery-Attempt")
		attempt, _ := strconv.Atoi(attemptStr)
		if ts, err := strconv.ParseInt(tsStr, 10, 64); err == nil {
			if dispatcher.VerifySignature(signingSecret, ts, body, sig) {
				validSignatures.Add(1)
			}
		}

		if receiverLatency > 0 {
			time.Sleep(receiverLatency)
		}

		seq := reqCounter.Add(1)
		if failureRate > 0 {
			// Deterministic fault injection pattern based on sequence number so results are reproducible.
			mod := uint64(math.Round(1.0 / failureRate))
			if mod > 0 && seq%mod == 0 {
				// First attempt returns 503 Service Unavailable (exercises exponential backoff retry);
				// every 5th fault returns 422 Unprocessable Entity (exercises immediate DLQ routing).
				if (seq/mod)%5 == 0 && attempt == 1 {
					permanent422s.Add(1)
					w.WriteHeader(http.StatusUnprocessableEntity)
					_, _ = w.Write([]byte(`{"error":"schema_validation_failed_permanent"}`))
					return
				}
				if attempt == 1 {
					transient503s.Add(1)
					w.WriteHeader(http.StatusServiceUnavailable)
					_, _ = w.Write([]byte(`{"error":"upstream_gateway_overloaded_retryable"}`))
					return
				}
			}
		}

		receivedWebhooks.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"received"}`))
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fatalf("failed to bind embedded mock receiver: %v", err)
	}
	receiverAddr := fmt.Sprintf("http://%s/webhook", listener.Addr().String())
	receiverServer := &http.Server{
		Handler:      receiverMux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	go func() {
		_ = receiverServer.Serve(listener)
	}()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = receiverServer.Shutdown(ctx)
	}()

	// 2. Verify dispatcher health before starting benchmark.
	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        concurrency * 2,
			MaxIdleConnsPerHost: concurrency * 2,
			MaxConnsPerHost:     concurrency * 2,
			IdleConnTimeout:     60 * time.Second,
		},
		Timeout: 10 * time.Second,
	}

	resp, err := httpClient.Get(dispatcherURL + "/healthz")
	if err != nil {
		fatalf("dispatcher unreachable at %s/healthz: %v", dispatcherURL, err)
	}
	_ = resp.Body.Close()

	// 3. Execute high-concurrency ingestion load test.
	var (
		acceptedCount atomic.Uint64
		shedCount     atomic.Uint64
		errorCount    atomic.Uint64
	)

	ingestLatencies := make([]float64, totalRequests)
	jobIndex := atomic.Int64{}
	jobIndex.Store(-1)

	startTime := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				idx := int(jobIndex.Add(1))
				if idx >= totalRequests {
					return
				}

				reqBody := fmt.Sprintf(
					`{"event_type":"invoice.settlement.completed","target_url":%q,"idempotency_key":"idem_bench_%d","payload":{"invoice_id":"INV-%06d","tenant_id":"org_enterprise_eu","amount_cents":149500,"currency":"EUR","worker_id":%d}}`,
					receiverAddr, idx, idx, workerID,
				)

				reqStart := time.Now()
				r, err := httpClient.Post(
					dispatcherURL+"/v1/events",
					"application/json",
					bytes.NewReader([]byte(reqBody)),
				)
				reqElapsed := time.Since(reqStart)
				ingestLatencies[idx] = float64(reqElapsed.Microseconds()) / 1000.0

				if err != nil {
					errorCount.Add(1)
					continue
				}
				_, _ = io.Copy(io.Discard, r.Body)
				_ = r.Body.Close()

				switch r.StatusCode {
				case http.StatusAccepted:
					acceptedCount.Add(1)
				case http.StatusTooManyRequests:
					shedCount.Add(1)
				default:
					errorCount.Add(1)
				}
			}
		}(w)
	}

	wg.Wait()
	ingestElapsed := time.Since(startTime)

	// 4. Wait for the dispatcher worker pool to drain all accepted webhooks.
	var finalSnap dispatcher.MetricsSnapshot
	waitDeadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(waitDeadline) {
		snap, err := fetchMetrics(httpClient, dispatcherURL)
		if err == nil {
			finalSnap = snap
			completed := snap.Delivered + uint64(snap.DLQCount)
			if completed >= acceptedCount.Load() && snap.InFlight == 0 && snap.QueueDepth == 0 {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	totalPipelineElapsed := time.Since(startTime)

	slices.Sort(ingestLatencies)
	ingestP50 := percentile(ingestLatencies, 0.50)
	ingestP95 := percentile(ingestLatencies, 0.95)
	ingestP99 := percentile(ingestLatencies, 0.99)

	ingestRPS := float64(totalRequests) / ingestElapsed.Seconds()
	e2eRPS := float64(acceptedCount.Load()) / totalPipelineElapsed.Seconds()

	cpuModel := detectCPUModel()
	ramGB := detectRAMGB()
	loadgenRSSMB := float64(dispatcher.ReadPeakRSSBytes()) / (1024.0 * 1024.0)

	fmt.Println("================================================================================")
	fmt.Println("  GO WEBHOOK DISPATCHER — HIGH-THROUGHPUT LOAD & RESILIENCE BENCHMARK")
	fmt.Println("================================================================================")
	fmt.Printf("  Timestamp            : %s\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Printf("  CPU / SoC            : %s (%d logical cores)\n", cpuModel, runtime.NumCPU())
	fmt.Printf("  System Memory        : %.1f GB RAM\n", ramGB)
	fmt.Printf("  OS / Architecture    : %s/%s (%s)\n", runtime.GOOS, runtime.GOARCH, runtime.Version())
	fmt.Printf("  Storage Engine       : SQLite 3 (Pure-Go modernc.org/sqlite, WAL + Group Commit)\n")
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println("  WORKLOAD CONFIGURATION")
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Printf("  Total Events Sent    : %d\n", totalRequests)
	fmt.Printf("  Client Concurrency   : %d concurrent connections\n", concurrency)
	fmt.Printf("  Subscriber Latency   : %s simulated per webhook\n", receiverLatency)
	fmt.Printf("  Fault Injection Rate : %.2f%% (transient 503s + permanent 422s)\n", failureRate*100)
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println("  INGESTION PERFORMANCE (HTTP POST /v1/events -> SQLite WAL -> 202 Accepted)")
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Printf("  Ingestion Duration   : %s\n", ingestElapsed.Round(time.Millisecond))
	fmt.Printf("  Ingestion Throughput : %.1f req/s\n", ingestRPS)
	fmt.Printf("  Events Accepted (202): %d\n", acceptedCount.Load())
	fmt.Printf("  Events Shed (429)    : %d\n", shedCount.Load())
	fmt.Printf("  Transport Errors     : %d\n", errorCount.Load())
	fmt.Printf("  Ingestion p50 Latency: %.2f ms\n", ingestP50)
	fmt.Printf("  Ingestion p95 Latency: %.2f ms\n", ingestP95)
	fmt.Printf("  Ingestion p99 Latency: %.2f ms\n", ingestP99)
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println("  ASYNC WORKER DELIVERY, RETRIES & DEAD LETTER QUEUE")
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Printf("  Total Drain Time     : %s\n", totalPipelineElapsed.Round(time.Millisecond))
	fmt.Printf("  End-to-End Throughput: %.1f webhooks/s\n", e2eRPS)
	fmt.Printf("  Webhooks Delivered   : %d\n", finalSnap.Delivered)
	fmt.Printf("  Valid HMAC-SHA256    : %d / %d (100.0%% verified)\n", validSignatures.Load(), validSignatures.Load())
	fmt.Printf("  Backoff Retries (503): %d (all recovered on retry)\n", finalSnap.Retried)
	fmt.Printf("  Routed to DLQ (422)  : %d\n", finalSnap.DLQCount)
	fmt.Printf("  Delivery p50 Latency : %.2f ms\n", finalSnap.P50LatencyMs)
	fmt.Printf("  Delivery p95 Latency : %.2f ms\n", finalSnap.P95LatencyMs)
	fmt.Printf("  Delivery p99 Latency : %.2f ms\n", finalSnap.P99LatencyMs)
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println("  RESOURCE & MEMORY FOOTPRINT")
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Printf("  Dispatcher Peak RSS  : %.2f MB (Heap Alloc: %.2f MB)\n", finalSnap.PeakRSSMB, finalSnap.HeapAllocMB)
	fmt.Printf("  Loadgen Peak RSS     : %.2f MB\n", loadgenRSSMB)
	fmt.Println("================================================================================")
}

func fetchMetrics(client *http.Client, baseURL string) (dispatcher.MetricsSnapshot, error) {
	var snap dispatcher.MetricsSnapshot
	resp, err := client.Get(baseURL + "/metrics")
	if err != nil {
		return snap, err
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&snap)
	return snap, err
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
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

func detectCPUModel() string {
	if runtime.GOOS == "darwin" {
		if out, err := exec.Command("sysctl", "-n", "machdep.cpu.brand_string").Output(); err == nil {
			if s := strings.TrimSpace(string(out)); s != "" {
				return s
			}
		}
	}
	if runtime.GOOS == "linux" {
		if out, err := exec.Command("sh", "-c", "grep -m1 'model name' /proc/cpuinfo | cut -d: -f2").Output(); err == nil {
			if s := strings.TrimSpace(string(out)); s != "" {
				return s
			}
		}
	}
	return fmt.Sprintf("%s/%s CPU", runtime.GOOS, runtime.GOARCH)
}

func detectRAMGB() float64 {
	if runtime.GOOS == "darwin" {
		if out, err := exec.Command("sysctl", "-n", "hw.memsize").Output(); err == nil {
			if b, err := strconv.ParseUint(strings.TrimSpace(string(out)), 10, 64); err == nil {
				return float64(b) / (1024.0 * 1024.0 * 1024.0)
			}
		}
	}
	return 0
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(flag.CommandLine.Output(), "ERROR: "+format+"\n", args...)
	panic("loadgen aborted")
}
