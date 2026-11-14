package dispatcher

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"
)

func (e *Engine) startWorkers() {
	for i := 0; i < e.cfg.WorkerCount; i++ {
		e.workerWg.Add(1)
		go e.workerLoop(i)
	}
}

func (e *Engine) workerLoop(_ int) {
	defer e.workerWg.Done()

	for evt := range e.jobCh {
		// Release one slot from the bounded ingestion semaphore as the job leaves the buffer.
		<-e.slots

		e.metrics.IncInFlight()
		e.processEvent(evt)
		e.metrics.DecInFlight()
	}
}

func (e *Engine) processEvent(evt Event) {
	maxRetries := evt.MaxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}

	totalAttempts := maxRetries + 1
	for attempt := 1; attempt <= totalAttempts; attempt++ {
		start := time.Now()
		statusCode, respSnippet, err := e.dispatchHTTP(evt, attempt)
		elapsed := time.Since(start)
		latencyMs := float64(elapsed.Microseconds()) / 1000.0

		dbCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		if err == nil && statusCode >= 200 && statusCode < 300 {
			_ = e.store.MarkDelivered(dbCtx, evt.ID, attempt, statusCode, latencyMs)
			cancel()
			e.metrics.RecordDeliveryLatency(elapsed)
			e.metrics.IncDelivered()
			return
		}

		errMsg := formatDeliveryError(statusCode, respSnippet, err)

		// Immediate DLQ routing for 4xx client errors (non-retriable permanent failures).
		if err == nil && statusCode >= 400 && statusCode < 500 {
			_ = e.store.MarkDLQ(dbCtx, evt.ID, attempt, statusCode, errMsg, "PERMANENT_CLIENT_ERROR_4XX")
			cancel()
			e.metrics.IncDLQ()
			return
		}

		// Retriable failure (5xx server error or network/timeout error).
		if attempt <= maxRetries {
			_ = e.store.MarkRetrying(dbCtx, evt.ID, attempt, statusCode, errMsg)
			cancel()
			e.metrics.IncRetried()

			backoff := computeBackoffWithJitter(e.cfg.BaseBackoff, e.cfg.MaxBackoff, attempt)
			timer := time.NewTimer(backoff)
			select {
			case <-timer.C:
			case <-e.drainCtx.Done():
				timer.Stop()
				failCtx, failCancel := context.WithTimeout(context.Background(), 2*time.Second)
				_ = e.store.MarkDLQ(failCtx, evt.ID, attempt, statusCode, errMsg+" (aborted on shutdown drain timeout)", "SHUTDOWN_DRAIN_EXCEEDED")
				failCancel()
				e.metrics.IncDLQ()
				return
			}
			continue
		}

		// Retries exhausted -> route to Dead Letter Queue.
		_ = e.store.MarkDLQ(dbCtx, evt.ID, attempt, statusCode, errMsg, "MAX_RETRIES_EXCEEDED")
		cancel()
		e.metrics.IncDLQ()
		return
	}
}

func (e *Engine) dispatchHTTP(evt Event, attempt int) (int, string, error) {
	reqCtx, cancel := context.WithTimeout(e.drainCtx, e.cfg.DeliveryTimeout)
	defer cancel()

	timestamp := time.Now().UTC().Unix()
	secret := evt.Secret
	if secret == "" {
		secret = e.cfg.DefaultSecret
	}
	signature := ComputeSignature(secret, timestamp, evt.Payload)

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, evt.TargetURL, bytes.NewReader(evt.Payload))
	if err != nil {
		return 0, "", fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Go-Webhook-Dispatcher/1.0")
	req.Header.Set("X-Webhook-ID", evt.ID)
	req.Header.Set("X-Event-Type", evt.EventType)
	req.Header.Set("X-Idempotency-Key", evt.IdempotencyKey)
	req.Header.Set("X-Webhook-Timestamp", strconv.FormatInt(timestamp, 10))
	req.Header.Set("X-Signature-256", signature)
	req.Header.Set("X-Delivery-Attempt", strconv.Itoa(attempt))

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()

	// Drain up to 4KB to capture diagnostic text and enable TCP connection reuse.
	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	_, _ = io.Copy(io.Discard, resp.Body)

	return resp.StatusCode, string(bytes.TrimSpace(bodyBytes)), nil
}

// computeBackoffWithJitter calculates full-jitter exponential backoff:
// sleep = random(base/2, min(maxBackoff, base * 2^(attempt-1))).
func computeBackoffWithJitter(base, maxBackoff time.Duration, attempt int) time.Duration {
	if base <= 0 {
		base = 25 * time.Millisecond
	}
	if maxBackoff < base {
		maxBackoff = base
	}
	shift := attempt - 1
	if shift > 10 {
		shift = 10
	}
	ceil := float64(base) * math.Pow(2, float64(shift))
	if ceil > float64(maxBackoff) {
		ceil = float64(maxBackoff)
	}
	floor := ceil * 0.5
	jittered := floor + rand.Float64()*(ceil-floor)
	return time.Duration(jittered)
}

func formatDeliveryError(statusCode int, body string, err error) string {
	if err != nil {
		return err.Error()
	}
	if body != "" {
		if len(body) > 256 {
			body = body[:256] + "..."
		}
		return fmt.Sprintf("HTTP %d: %s", statusCode, body)
	}
	return fmt.Sprintf("HTTP %d %s", statusCode, http.StatusText(statusCode))
}
