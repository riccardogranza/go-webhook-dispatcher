package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

const maxRequestBodyBytes = 1 << 20 // 1 MiB

// Engine coordinates HTTP ingestion, SQLite WAL persistence, bounded worker pools, and DLQ replay.
type Engine struct {
	cfg         Config
	store       *Store
	metrics     *Metrics
	slots       chan struct{}
	jobCh       chan Event
	httpClient  *http.Client
	mux         *http.ServeMux
	enqueueMu   sync.RWMutex
	closing     atomic.Bool
	shutdownOne sync.Once
	workerWg    sync.WaitGroup
	drainCtx    context.Context
	drainCancel context.CancelFunc
}

// NewEngine initializes the SQLite WAL store, bounded channels, HTTP transport, and worker pool.
func NewEngine(cfg Config) (*Engine, error) {
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 16
	}
	if cfg.QueueCapacity <= 0 {
		cfg.QueueCapacity = 1024
	}
	if cfg.RetryAfterSeconds <= 0 {
		cfg.RetryAfterSeconds = 1
	}
	if cfg.DeliveryTimeout <= 0 {
		cfg.DeliveryTimeout = 5 * time.Second
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 15 * time.Second
	}

	store, err := NewStore(cfg.DBPath, cfg.MaxBatchSize)
	if err != nil {
		return nil, err
	}

	metrics := NewMetrics()
	if dlqInitial, err := store.CountDLQ(context.Background()); err == nil {
		metrics.SetDLQCount(dlqInitial)
	}

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   3 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     false,
		MaxIdleConns:          cfg.WorkerCount * 4,
		MaxIdleConnsPerHost:   cfg.WorkerCount * 2,
		MaxConnsPerHost:       cfg.WorkerCount * 4,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	drainCtx, drainCancel := context.WithCancel(context.Background())

	eng := &Engine{
		cfg:     cfg,
		store:   store,
		metrics: metrics,
		slots:   make(chan struct{}, cfg.QueueCapacity),
		jobCh:   make(chan Event, cfg.QueueCapacity),
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   cfg.DeliveryTimeout,
		},
		mux:         http.NewServeMux(),
		drainCtx:    drainCtx,
		drainCancel: drainCancel,
	}

	eng.registerRoutes()
	eng.startWorkers()
	return eng, nil
}

// Handler returns the HTTP handler for the engine.
func (e *Engine) Handler() http.Handler {
	return e.mux
}

// Metrics returns the engine's telemetry collector.
func (e *Engine) Metrics() *Metrics {
	return e.metrics
}

func (e *Engine) registerRoutes() {
	e.mux.HandleFunc("POST /v1/events", e.handleIngestEvent)
	e.mux.HandleFunc("GET /v1/events/{id}", e.handleGetEvent)
	e.mux.HandleFunc("GET /v1/dlq", e.handleListDLQ)
	e.mux.HandleFunc("POST /v1/dlq/{id}/replay", e.handleReplayDLQ)
	e.mux.HandleFunc("GET /metrics", e.handleMetrics)
	e.mux.HandleFunc("GET /healthz", e.handleHealthz)
}

func (e *Engine) handleIngestEvent(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	defer r.Body.Close()

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error":   "invalid_request_body",
			"message": "failed to read request body or payload exceeds 1 MiB limit",
		})
		return
	}

	var req IngestEventRequest
	dec := json.NewDecoder(strings.NewReader(string(bodyBytes)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error":   "malformed_json",
			"message": err.Error(),
		})
		return
	}

	if err := req.Validate(); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error":   "validation_failed",
			"message": err.Error(),
		})
		return
	}

	e.enqueueMu.RLock()
	defer e.enqueueMu.RUnlock()

	if e.closing.Load() {
		w.Header().Set("Retry-After", strconv.Itoa(e.cfg.RetryAfterSeconds))
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error":   "server_shutting_down",
			"message": "dispatcher is draining in-flight events and rejecting new writes",
		})
		return
	}

	// Explicit backpressure: immediately shed load if bounded queue buffer is full.
	select {
	case e.slots <- struct{}{}:
	default:
		e.metrics.IncShed()
		w.Header().Set("Retry-After", strconv.Itoa(e.cfg.RetryAfterSeconds))
		writeJSON(w, http.StatusTooManyRequests, map[string]any{
			"error":       "backpressure_shed_load",
			"message":     "bounded worker channel is at maximum capacity; retry after backoff",
			"retry_after": e.cfg.RetryAfterSeconds,
		})
		return
	}

	now := time.Now().UTC()
	idempotencyKey := strings.TrimSpace(req.IdempotencyKey)
	if idempotencyKey == "" {
		if headerKey := strings.TrimSpace(r.Header.Get("Idempotency-Key")); headerKey != "" {
			idempotencyKey = headerKey
		} else {
			idempotencyKey = "idem_" + uuid.NewString()
		}
	}

	secret := req.Secret
	if secret == "" {
		secret = e.cfg.DefaultSecret
	}

	maxRetries := e.cfg.MaxRetries
	if req.MaxRetries != nil {
		maxRetries = *req.MaxRetries
	}

	evt := Event{
		ID:             "evt_" + uuid.NewString(),
		EventType:      req.EventType,
		TargetURL:      req.TargetURL,
		Payload:        req.Payload,
		Secret:         secret,
		IdempotencyKey: idempotencyKey,
		Status:         StatusPending,
		Attempts:       0,
		MaxRetries:     maxRetries,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	if err := e.store.InsertEvent(r.Context(), evt); err != nil {
		<-e.slots
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error":   "persistence_failure",
			"message": fmt.Sprintf("failed to persist event to SQLite WAL: %v", err),
		})
		return
	}

	e.metrics.IncEnqueued()
	e.jobCh <- evt

	writeJSON(w, http.StatusAccepted, IngestEventResponse{
		ID:             evt.ID,
		EventType:      evt.EventType,
		IdempotencyKey: evt.IdempotencyKey,
		Status:         StatusPending,
		EnqueuedAt:     now,
	})
}

func (e *Engine) handleGetEvent(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing_event_id"})
		return
	}

	evt, err := e.store.GetEvent(r.Context(), id)
	if errors.Is(err, ErrEventNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "event_not_found", "id": id})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "query_failed", "message": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, evt)
}

func (e *Engine) handleListDLQ(w http.ResponseWriter, r *http.Request) {
	limit := 50
	offset := 0
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if parsed, err := strconv.Atoi(o); err == nil && parsed >= 0 {
			offset = parsed
		}
	}

	items, total, err := e.store.ListDLQ(r.Context(), limit, offset)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error":   "dlq_query_failed",
			"message": err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, DLQListResponse{
		TotalCount: total,
		Limit:      limit,
		Offset:     offset,
		Items:      items,
	})
}

func (e *Engine) handleReplayDLQ(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing_event_id"})
		return
	}

	e.enqueueMu.RLock()
	defer e.enqueueMu.RUnlock()

	if e.closing.Load() {
		w.Header().Set("Retry-After", strconv.Itoa(e.cfg.RetryAfterSeconds))
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "server_shutting_down"})
		return
	}

	select {
	case e.slots <- struct{}{}:
	default:
		e.metrics.IncShed()
		w.Header().Set("Retry-After", strconv.Itoa(e.cfg.RetryAfterSeconds))
		writeJSON(w, http.StatusTooManyRequests, map[string]any{
			"error":       "backpressure_shed_load",
			"retry_after": e.cfg.RetryAfterSeconds,
		})
		return
	}

	evt, err := e.store.ReplayFromDLQ(r.Context(), id)
	if err != nil {
		<-e.slots
		if errors.Is(err, ErrEventNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "event_not_found", "id": id})
			return
		}
		if errors.Is(err, ErrEventNotInDLQ) {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "event_not_in_dlq", "id": id})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "replay_failed", "message": err.Error()})
		return
	}

	e.metrics.DecDLQOnReplay()
	e.metrics.IncEnqueued()
	e.jobCh <- *evt

	writeJSON(w, http.StatusAccepted, ReplayResponse{
		ID:         evt.ID,
		Status:     StatusPending,
		ReplayedAt: evt.UpdatedAt,
	})
}

func (e *Engine) handleMetrics(w http.ResponseWriter, r *http.Request) {
	snap := e.metrics.Snapshot(len(e.jobCh), cap(e.jobCh))
	format := r.URL.Query().Get("format")
	accept := r.Header.Get("Accept")
	if format == "prometheus" || strings.Contains(accept, "text/plain") || strings.Contains(accept, "application/openmetrics-text") {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(snap.FormatPrometheus()))
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (e *Engine) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	status := "ok"
	code := http.StatusOK
	if e.closing.Load() {
		status = "draining"
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, map[string]any{
		"status":      status,
		"queue_depth": len(e.jobCh),
		"in_flight":   e.metrics.inFlight.Load(),
	})
}

// Shutdown gracefully stops accepting new events, drains in-flight workers within ctx, and flushes SQLite WAL.
func (e *Engine) Shutdown(ctx context.Context) error {
	var shutdownErr error
	e.shutdownOne.Do(func() {
		e.enqueueMu.Lock()
		e.closing.Store(true)
		close(e.jobCh)
		e.enqueueMu.Unlock()

		doneCh := make(chan struct{})
		go func() {
			e.workerWg.Wait()
			close(doneCh)
		}()

		select {
		case <-doneCh:
		case <-ctx.Done():
			e.drainCancel()
			<-doneCh
			shutdownErr = ctx.Err()
		}

		e.httpClient.CloseIdleConnections()
		if err := e.store.Close(); err != nil && shutdownErr == nil {
			shutdownErr = err
		}
	})
	return shutdownErr
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}
