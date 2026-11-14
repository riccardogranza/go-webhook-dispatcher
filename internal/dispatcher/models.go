package dispatcher

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// EventStatus represents the lifecycle state of a webhook event.
type EventStatus string

const (
	StatusPending   EventStatus = "PENDING"
	StatusRetrying  EventStatus = "RETRYING"
	StatusDelivered EventStatus = "DELIVERED"
	StatusDLQ       EventStatus = "DLQ"
)

// IngestEventRequest represents the incoming JSON payload for POST /v1/events.
type IngestEventRequest struct {
	EventType      string          `json:"event_type"`
	TargetURL      string          `json:"target_url"`
	Payload        json.RawMessage `json:"payload"`
	Secret         string          `json:"secret,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	MaxRetries     *int            `json:"max_retries,omitempty"`
}

// Validate checks that the ingestion request is well-formed and safe to dispatch.
func (r *IngestEventRequest) Validate() error {
	if strings.TrimSpace(r.EventType) == "" {
		return errors.New("event_type is required and cannot be empty")
	}
	if len(r.EventType) > 128 {
		return errors.New("event_type exceeds maximum length of 128 characters")
	}
	if strings.TrimSpace(r.TargetURL) == "" {
		return errors.New("target_url is required")
	}
	parsedURL, err := url.ParseRequestURI(r.TargetURL)
	if err != nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") || parsedURL.Host == "" {
		return fmt.Errorf("target_url must be a valid http or https URL: %q", r.TargetURL)
	}
	if len(r.Payload) == 0 || !json.Valid(r.Payload) {
		return errors.New("payload must be a non-empty valid JSON value")
	}
	trimmed := strings.TrimSpace(string(r.Payload))
	if trimmed == "null" {
		return errors.New("payload cannot be null")
	}
	if r.MaxRetries != nil && (*r.MaxRetries < 0 || *r.MaxRetries > 15) {
		return errors.New("max_retries must be between 0 and 15")
	}
	return nil
}

// Event represents a persisted webhook event in SQLite and in the worker pipeline.
type Event struct {
	ID             string          `json:"id"`
	EventType      string          `json:"event_type"`
	TargetURL      string          `json:"target_url"`
	Payload        json.RawMessage `json:"payload"`
	Secret         string          `json:"-"`
	IdempotencyKey string          `json:"idempotency_key"`
	Status         EventStatus     `json:"status"`
	Attempts       int             `json:"attempts"`
	MaxRetries     int             `json:"max_retries"`
	LastStatusCode int             `json:"last_status_code,omitempty"`
	LastError      string          `json:"last_error,omitempty"`
	DLQReason      string          `json:"dlq_reason,omitempty"`
	DeliveryMs     float64         `json:"delivery_latency_ms,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	DeliveredAt    *time.Time      `json:"delivered_at,omitempty"`
	FailedAt       *time.Time      `json:"failed_at,omitempty"`
}

// IngestEventResponse is returned with HTTP 202 Accepted upon durable enqueueing.
type IngestEventResponse struct {
	ID             string      `json:"id"`
	EventType      string      `json:"event_type"`
	IdempotencyKey string      `json:"idempotency_key"`
	Status         EventStatus `json:"status"`
	EnqueuedAt     time.Time   `json:"enqueued_at"`
}

// DLQListResponse is returned by GET /v1/dlq.
type DLQListResponse struct {
	TotalCount int     `json:"total_count"`
	Limit      int     `json:"limit"`
	Offset     int     `json:"offset"`
	Items      []Event `json:"items"`
}

// ReplayResponse is returned by POST /v1/dlq/{id}/replay.
type ReplayResponse struct {
	ID         string      `json:"id"`
	Status     EventStatus `json:"status"`
	ReplayedAt time.Time   `json:"replayed_at"`
}
