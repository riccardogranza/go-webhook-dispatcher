package dispatcher_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/riccardogranza/go-webhook-dispatcher/internal/dispatcher"
)

func newTestEngine(t *testing.T, override func(*dispatcher.Config)) (*dispatcher.Engine, *httptest.Server) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test_dispatcher.db")
	cfg := dispatcher.DefaultConfig()
	cfg.DBPath = dbPath
	cfg.WorkerCount = 4
	cfg.QueueCapacity = 64
	cfg.MaxRetries = 3
	cfg.BaseBackoff = 10 * time.Millisecond
	cfg.MaxBackoff = 40 * time.Millisecond
	cfg.DeliveryTimeout = 1 * time.Second
	cfg.ShutdownTimeout = 5 * time.Second
	cfg.DefaultSecret = "whsec_test_secret_key_for_hmac_sha256"
	if override != nil {
		override(&cfg)
	}

	eng, err := dispatcher.NewEngine(cfg)
	if err != nil {
		t.Fatalf("NewEngine failed: %v", err)
	}
	srv := httptest.NewServer(eng.Handler())

	t.Cleanup(func() {
		srv.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = eng.Shutdown(ctx)
	})

	return eng, srv
}

func TestHappyPathAndHMACVerification(t *testing.T) {
	const secret = "whsec_custom_endpoint_secret"
	deliveredCh := make(chan struct{}, 1)

	var (
		gotSig         string
		gotIdempotency string
		gotTimestamp   int64
		gotBody        []byte
	)

	subscriber := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSig = r.Header.Get("X-Signature-256")
		gotIdempotency = r.Header.Get("X-Idempotency-Key")
		gotTimestamp, _ = strconv.ParseInt(r.Header.Get("X-Webhook-Timestamp"), 10, 64)
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
		select {
		case deliveredCh <- struct{}{}:
		default:
		}
	}))
	defer subscriber.Close()

	_, srv := newTestEngine(t, nil)

	reqPayload := map[string]any{
		"event_type":      "order.created",
		"target_url":      subscriber.URL,
		"secret":          secret,
		"idempotency_key": "idem_order_991",
		"payload": map[string]any{
			"order_id": "ORD-991",
			"total":    2450.00,
		},
	}
	rawBody, _ := json.Marshal(reqPayload)

	resp, err := http.Post(srv.URL+"/v1/events", "application/json", bytes.NewReader(rawBody))
	if err != nil {
		t.Fatalf("POST /v1/events failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d", resp.StatusCode)
	}

	var accepted dispatcher.IngestEventResponse
	if err := json.NewDecoder(resp.Body).Decode(&accepted); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	select {
	case <-deliveredCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for webhook delivery")
	}

	if !dispatcher.VerifySignature(secret, gotTimestamp, gotBody, gotSig) {
		t.Fatalf("HMAC-SHA256 signature verification failed: header=%s", gotSig)
	}
	if gotIdempotency != "idem_order_991" {
		t.Fatalf("expected X-Idempotency-Key=idem_order_991, got %s", gotIdempotency)
	}

	// Poll until SQLite state reflects DELIVERED.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		evtResp, err := http.Get(srv.URL + "/v1/events/" + accepted.ID)
		if err == nil {
			var evt dispatcher.Event
			_ = json.NewDecoder(evtResp.Body).Decode(&evt)
			_ = evtResp.Body.Close()
			if evt.Status == dispatcher.StatusDelivered {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Check JSON and Prometheus metrics.
	mResp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics failed: %v", err)
	}
	defer mResp.Body.Close()
	var snap dispatcher.MetricsSnapshot
	if err := json.NewDecoder(mResp.Body).Decode(&snap); err != nil {
		t.Fatalf("decode metrics: %v", err)
	}
	if snap.Enqueued != 1 || snap.Delivered != 1 {
		t.Fatalf("unexpected metrics snapshot: %+v", snap)
	}

	promResp, err := http.Get(srv.URL + "/metrics?format=prometheus")
	if err != nil {
		t.Fatalf("GET /metrics?format=prometheus failed: %v", err)
	}
	defer promResp.Body.Close()
	promBytes, _ := io.ReadAll(promResp.Body)
	if !strings.Contains(string(promBytes), "webhook_dispatcher_delivered_total 1") {
		t.Fatalf("expected prometheus counter in output, got:\n%s", string(promBytes))
	}
}

func TestExplicitBackpressureHTTP429(t *testing.T) {
	blockCh := make(chan struct{})
	subscriber := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-blockCh
		w.WriteHeader(http.StatusOK)
	}))
	defer subscriber.Close()
	defer close(blockCh)

	_, srv := newTestEngine(t, func(cfg *dispatcher.Config) {
		cfg.WorkerCount = 1
		cfg.QueueCapacity = 2
		cfg.RetryAfterSeconds = 3
	})

	payload := []byte(`{"event_type":"load.test","target_url":"` + subscriber.URL + `","payload":{"seq":1}}`)

	var (
		accepted int32
		shed429  int32
		retryHdr atomic.Value
		wg       sync.WaitGroup
	)

	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := http.Post(srv.URL+"/v1/events", "application/json", bytes.NewReader(payload))
			if err != nil {
				return
			}
			defer r.Body.Close()
			if r.StatusCode == http.StatusAccepted {
				atomic.AddInt32(&accepted, 1)
			} else if r.StatusCode == http.StatusTooManyRequests {
				atomic.AddInt32(&shed429, 1)
				retryHdr.Store(r.Header.Get("Retry-After"))
			}
		}()
	}
	wg.Wait()

	if atomic.LoadInt32(&shed429) == 0 {
		t.Fatalf("expected backpressure 429 responses when queue is full, got 0 (accepted=%d)", accepted)
	}
	if retryHdr.Load() != "3" {
		t.Fatalf("expected Retry-After header '3', got %v", retryHdr.Load())
	}
}

func TestExponentialBackoffRetryAndRecovery(t *testing.T) {
	var attempts atomic.Int32
	deliveredCh := make(chan struct{}, 1)

	subscriber := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := attempts.Add(1)
		if n < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"temporary_overload"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		select {
		case deliveredCh <- struct{}{}:
		default:
		}
	}))
	defer subscriber.Close()

	eng, srv := newTestEngine(t, func(cfg *dispatcher.Config) {
		cfg.MaxRetries = 3
		cfg.BaseBackoff = 10 * time.Millisecond
		cfg.MaxBackoff = 25 * time.Millisecond
	})

	body := []byte(`{"event_type":"payment.retry_test","target_url":"` + subscriber.URL + `","payload":{"id":"PAY-1"}}`)
	resp, err := http.Post(srv.URL+"/v1/events", "application/json", bytes.NewReader(body))
	if err != nil || resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, err=%v", err)
	}
	_ = resp.Body.Close()

	select {
	case <-deliveredCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for retry recovery")
	}

	if attempts.Load() != 3 {
		t.Fatalf("expected 3 total attempts, got %d", attempts.Load())
	}

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		snap := eng.Metrics().Snapshot(0, 64)
		if snap.Delivered == 1 && snap.Retried == 2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected delivered=1 and retried=2, got %+v", eng.Metrics().Snapshot(0, 64))
}

func TestImmediateDLQOn4xxAndReplay(t *testing.T) {
	var shouldSucceed atomic.Bool
	var callCount atomic.Int32

	subscriber := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount.Add(1)
		if !shouldSucceed.Load() {
			w.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = w.Write([]byte(`{"error":"invalid_subscriber_schema"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer subscriber.Close()

	eng, srv := newTestEngine(t, func(cfg *dispatcher.Config) {
		cfg.MaxRetries = 4
	})

	body := []byte(`{"event_type":"user.provision","target_url":"` + subscriber.URL + `","payload":{"user_id":"u_42"}}`)
	resp, err := http.Post(srv.URL+"/v1/events", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("ingest failed: %v", err)
	}
	var accepted dispatcher.IngestEventResponse
	_ = json.NewDecoder(resp.Body).Decode(&accepted)
	_ = resp.Body.Close()

	// Wait for event to enter DLQ immediately without retries.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if eng.Metrics().Snapshot(0, 64).DLQCount == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if callCount.Load() != 1 {
		t.Fatalf("expected 4xx to route immediately to DLQ after 1 attempt without retry, got %d attempts", callCount.Load())
	}

	// Inspect GET /v1/dlq.
	dlqResp, err := http.Get(srv.URL + "/v1/dlq")
	if err != nil {
		t.Fatalf("GET /v1/dlq failed: %v", err)
	}
	var dlqList dispatcher.DLQListResponse
	_ = json.NewDecoder(dlqResp.Body).Decode(&dlqList)
	_ = dlqResp.Body.Close()

	if dlqList.TotalCount != 1 || len(dlqList.Items) != 1 || dlqList.Items[0].ID != accepted.ID {
		t.Fatalf("expected 1 DLQ item matching %s, got %+v", accepted.ID, dlqList)
	}
	if dlqList.Items[0].DLQReason != "PERMANENT_CLIENT_ERROR_4XX" {
		t.Fatalf("expected DLQReason=PERMANENT_CLIENT_ERROR_4XX, got %s", dlqList.Items[0].DLQReason)
	}

	// Fix subscriber and trigger POST /v1/dlq/{id}/replay.
	shouldSucceed.Store(true)
	replayResp, err := http.Post(srv.URL+"/v1/dlq/"+accepted.ID+"/replay", "application/json", nil)
	if err != nil {
		t.Fatalf("POST replay failed: %v", err)
	}
	_ = replayResp.Body.Close()
	if replayResp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted on DLQ replay, got %d", replayResp.StatusCode)
	}

	// Wait for replayed event to be delivered.
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snap := eng.Metrics().Snapshot(0, 64)
		if snap.Delivered == 1 && snap.DLQCount == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("replayed event did not complete delivery: %+v", eng.Metrics().Snapshot(0, 64))
}

func TestExhaustedRetriesRoutesToDLQ(t *testing.T) {
	var attempts atomic.Int32
	subscriber := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"database_down"}`))
	}))
	defer subscriber.Close()

	eng, srv := newTestEngine(t, func(cfg *dispatcher.Config) {
		cfg.MaxRetries = 2
		cfg.BaseBackoff = 5 * time.Millisecond
		cfg.MaxBackoff = 15 * time.Millisecond
	})

	body := []byte(`{"event_type":"invoice.failed","target_url":"` + subscriber.URL + `","payload":{"id":"INV-500"}}`)
	resp, err := http.Post(srv.URL+"/v1/events", "application/json", bytes.NewReader(body))
	if err != nil || resp.StatusCode != http.StatusAccepted {
		t.Fatalf("ingest failed: %v", err)
	}
	_ = resp.Body.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if eng.Metrics().Snapshot(0, 64).DLQCount == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if attempts.Load() != 3 {
		t.Fatalf("expected 3 attempts (1 initial + 2 retries), got %d", attempts.Load())
	}
}
