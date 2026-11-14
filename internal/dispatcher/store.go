package dispatcher

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// ErrEventNotFound indicates the requested event ID does not exist.
var ErrEventNotFound = errors.New("event not found")

// ErrEventNotInDLQ indicates the requested event is not currently in DLQ state.
var ErrEventNotInDLQ = errors.New("event is not in the dead letter queue")

type writeOpKind int

const (
	opInsert writeOpKind = iota
	opDelivered
	opRetrying
	opDLQ
	opReplay
)

type writeOp struct {
	kind       writeOpKind
	event      Event
	id         string
	attempts   int
	statusCode int
	lastError  string
	dlqReason  string
	deliveryMs float64
	timestamp  time.Time
	resultCh   chan *Event
	done       chan error
}

// Store manages durable SQLite WAL persistence with group-commit write coalescing.
type Store struct {
	writeDB      *sql.DB
	readDB       *sql.DB
	stmtInsert   *sql.Stmt
	stmtDeliver  *sql.Stmt
	stmtRetry    *sql.Stmt
	stmtDLQ      *sql.Stmt
	writeCh      chan writeOp
	maxBatchSize int
	wg           sync.WaitGroup
	closeOnce    sync.Once
}

const schemaSQL = `
CREATE TABLE IF NOT EXISTS events (
	id               TEXT PRIMARY KEY,
	event_type       TEXT NOT NULL,
	target_url       TEXT NOT NULL,
	payload          TEXT NOT NULL,
	secret           TEXT NOT NULL,
	idempotency_key  TEXT NOT NULL,
	status           TEXT NOT NULL,
	attempts         INTEGER NOT NULL DEFAULT 0,
	max_retries      INTEGER NOT NULL DEFAULT 3,
	last_status_code INTEGER NOT NULL DEFAULT 0,
	last_error       TEXT NOT NULL DEFAULT '',
	dlq_reason       TEXT NOT NULL DEFAULT '',
	delivery_ms      REAL NOT NULL DEFAULT 0,
	created_at        TEXT NOT NULL,
	updated_at       TEXT NOT NULL,
	delivered_at     TEXT,
	failed_at        TEXT
);

CREATE INDEX IF NOT EXISTS idx_events_status_created ON events(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_idempotency ON events(idempotency_key);
`

// NewStore opens the SQLite database in WAL mode and starts the group-commit writer goroutine.
func NewStore(dbPath string, maxBatchSize int) (*Store, error) {
	if maxBatchSize <= 0 {
		maxBatchSize = 128
	}

	writeDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open write sqlite db: %w", err)
	}
	writeDB.SetMaxOpenConns(1)
	writeDB.SetMaxIdleConns(1)
	writeDB.SetConnMaxLifetime(0)

	pragmas := []string{
		"PRAGMA journal_mode = WAL;",
		"PRAGMA synchronous = NORMAL;",
		"PRAGMA busy_timeout = 5000;",
		"PRAGMA temp_store = MEMORY;",
		"PRAGMA cache_size = -64000;",
		"PRAGMA foreign_keys = ON;",
	}
	for _, p := range pragmas {
		if _, err := writeDB.Exec(p); err != nil {
			_ = writeDB.Close()
			return nil, fmt.Errorf("exec pragma %q: %w", p, err)
		}
	}

	if _, err := writeDB.Exec(schemaSQL); err != nil {
		_ = writeDB.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}

	readDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		_ = writeDB.Close()
		return nil, fmt.Errorf("open read sqlite db: %w", err)
	}
	readConns := runtime.NumCPU()
	if readConns < 4 {
		readConns = 4
	}
	readDB.SetMaxOpenConns(readConns)
	readDB.SetMaxIdleConns(readConns)
	for _, p := range []string{
		"PRAGMA journal_mode = WAL;",
		"PRAGMA busy_timeout = 5000;",
		"PRAGMA query_only = ON;",
	} {
		if _, err := readDB.Exec(p); err != nil {
			_ = readDB.Close()
			_ = writeDB.Close()
			return nil, fmt.Errorf("exec read pragma %q: %w", p, err)
		}
	}

	stmtInsert, err := writeDB.Prepare(`
		INSERT INTO events (
			id, event_type, target_url, payload, secret, idempotency_key,
			status, attempts, max_retries, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		_ = readDB.Close()
		_ = writeDB.Close()
		return nil, fmt.Errorf("prepare insert: %w", err)
	}

	stmtDeliver, err := writeDB.Prepare(`
		UPDATE events
		SET status = ?, attempts = ?, last_status_code = ?, last_error = '',
		    delivery_ms = ?, updated_at = ?, delivered_at = ?
		WHERE id = ?
	`)
	if err != nil {
		_ = readDB.Close()
		_ = writeDB.Close()
		return nil, fmt.Errorf("prepare deliver: %w", err)
	}

	stmtRetry, err := writeDB.Prepare(`
		UPDATE events
		SET status = ?, attempts = ?, last_status_code = ?, last_error = ?, updated_at = ?
		WHERE id = ?
	`)
	if err != nil {
		_ = readDB.Close()
		_ = writeDB.Close()
		return nil, fmt.Errorf("prepare retry: %w", err)
	}

	stmtDLQ, err := writeDB.Prepare(`
		UPDATE events
		SET status = ?, attempts = ?, last_status_code = ?, last_error = ?,
		    dlq_reason = ?, updated_at = ?, failed_at = ?
		WHERE id = ?
	`)
	if err != nil {
		_ = readDB.Close()
		_ = writeDB.Close()
		return nil, fmt.Errorf("prepare dlq: %w", err)
	}

	s := &Store{
		writeDB:      writeDB,
		readDB:       readDB,
		stmtInsert:   stmtInsert,
		stmtDeliver:  stmtDeliver,
		stmtRetry:    stmtRetry,
		stmtDLQ:      stmtDLQ,
		writeCh:      make(chan writeOp, 8192),
		maxBatchSize: maxBatchSize,
	}

	s.wg.Add(1)
	go s.writeLoop()

	return s, nil
}

func (s *Store) writeLoop() {
	defer s.wg.Done()
	batch := make([]writeOp, 0, s.maxBatchSize)

	for first := range s.writeCh {
		batch = append(batch[:0], first)

	drain:
		for len(batch) < s.maxBatchSize {
			select {
			case next, ok := <-s.writeCh:
				if !ok {
					break drain
				}
				batch = append(batch, next)
			default:
				break drain
			}
		}

		s.commitBatch(batch)
	}
}

func (s *Store) commitBatch(batch []writeOp) {
	tx, err := s.writeDB.Begin()
	if err != nil {
		for _, op := range batch {
			op.done <- err
		}
		return
	}

	txInsert := tx.Stmt(s.stmtInsert)
	txDeliver := tx.Stmt(s.stmtDeliver)
	txRetry := tx.Stmt(s.stmtRetry)
	txDLQ := tx.Stmt(s.stmtDLQ)

	opErrs := make([]error, len(batch))
	for i := range batch {
		op := &batch[i]
		ts := op.timestamp.UTC().Format(time.RFC3339Nano)
		switch op.kind {
		case opInsert:
			e := op.event
			created := e.CreatedAt.UTC().Format(time.RFC3339Nano)
			_, opErrs[i] = txInsert.Exec(
				e.ID, e.EventType, e.TargetURL, string(e.Payload), e.Secret,
				e.IdempotencyKey, string(e.Status), e.Attempts, e.MaxRetries,
				created, created,
			)
		case opDelivered:
			_, opErrs[i] = txDeliver.Exec(
				string(StatusDelivered), op.attempts, op.statusCode,
				op.deliveryMs, ts, ts, op.id,
			)
		case opRetrying:
			_, opErrs[i] = txRetry.Exec(
				string(StatusRetrying), op.attempts, op.statusCode,
				op.lastError, ts, op.id,
			)
		case opDLQ:
			_, opErrs[i] = txDLQ.Exec(
				string(StatusDLQ), op.attempts, op.statusCode,
				op.lastError, op.dlqReason, ts, ts, op.id,
			)
		case opReplay:
			evt, err := replayInTx(tx, op.id, ts)
			opErrs[i] = err
			if op.resultCh != nil {
				op.resultCh <- evt
			}
		}
	}

	commitErr := tx.Commit()
	for i, op := range batch {
		if opErrs[i] != nil {
			op.done <- opErrs[i]
		} else {
			op.done <- commitErr
		}
	}
}

func replayInTx(tx *sql.Tx, id string, ts string) (*Event, error) {
	var e Event
	var payloadStr string
	var statusStr string
	var createdStr, updatedStr string
	var deliveredStr, failedStr sql.NullString

	err := tx.QueryRow(`
		SELECT id, event_type, target_url, payload, secret, idempotency_key,
		       status, attempts, max_retries, last_status_code, last_error,
		       dlq_reason, delivery_ms, created_at, updated_at, delivered_at, failed_at
		FROM events WHERE id = ?
	`, id).Scan(
		&e.ID, &e.EventType, &e.TargetURL, &payloadStr, &e.Secret, &e.IdempotencyKey,
		&statusStr, &e.Attempts, &e.MaxRetries, &e.LastStatusCode, &e.LastError,
		&e.DLQReason, &e.DeliveryMs, &createdStr, &updatedStr, &deliveredStr, &failedStr,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrEventNotFound
	}
	if err != nil {
		return nil, err
	}
	if EventStatus(statusStr) != StatusDLQ {
		return nil, ErrEventNotInDLQ
	}

	_, err = tx.Exec(`
		UPDATE events
		SET status = ?, attempts = 0, last_status_code = 0, last_error = '',
		    dlq_reason = '', updated_at = ?, failed_at = NULL
		WHERE id = ?
	`, string(StatusPending), ts, id)
	if err != nil {
		return nil, err
	}

	e.Payload = json.RawMessage(payloadStr)
	e.Status = StatusPending
	e.Attempts = 0
	e.LastStatusCode = 0
	e.LastError = ""
	e.DLQReason = ""
	e.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdStr)
	e.UpdatedAt, _ = time.Parse(time.RFC3339Nano, ts)
	return &e, nil
}

// InsertEvent durably persists a newly accepted event to SQLite WAL.
func (s *Store) InsertEvent(ctx context.Context, e Event) error {
	op := writeOp{
		kind:  opInsert,
		event: e,
		done:  make(chan error, 1),
	}
	select {
	case s.writeCh <- op:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-op.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// MarkDelivered updates an event's status to DELIVERED in SQLite WAL.
func (s *Store) MarkDelivered(ctx context.Context, id string, attempts, statusCode int, deliveryMs float64) error {
	op := writeOp{
		kind:       opDelivered,
		id:         id,
		attempts:   attempts,
		statusCode: statusCode,
		deliveryMs: deliveryMs,
		timestamp:  time.Now().UTC(),
		done:       make(chan error, 1),
	}
	select {
	case s.writeCh <- op:
	case <-ctx.Done():
		return ctx.Err()
	}
	return <-op.done
}

// MarkRetrying records a failed delivery attempt scheduled for backoff retry.
func (s *Store) MarkRetrying(ctx context.Context, id string, attempts, statusCode int, lastErr string) error {
	op := writeOp{
		kind:       opRetrying,
		id:         id,
		attempts:   attempts,
		statusCode: statusCode,
		lastError:  lastErr,
		timestamp:  time.Now().UTC(),
		done:       make(chan error, 1),
	}
	select {
	case s.writeCh <- op:
	case <-ctx.Done():
		return ctx.Err()
	}
	return <-op.done
}

// MarkDLQ routes a permanently failed event to the Dead Letter Queue in SQLite WAL.
func (s *Store) MarkDLQ(ctx context.Context, id string, attempts, statusCode int, lastErr, reason string) error {
	op := writeOp{
		kind:       opDLQ,
		id:         id,
		attempts:   attempts,
		statusCode: statusCode,
		lastError:  lastErr,
		dlqReason:  reason,
		timestamp:  time.Now().UTC(),
		done:       make(chan error, 1),
	}
	select {
	case s.writeCh <- op:
	case <-ctx.Done():
		return ctx.Err()
	}
	return <-op.done
}

// ReplayFromDLQ transitions a DLQ event back to PENDING and returns it for re-dispatch.
func (s *Store) ReplayFromDLQ(ctx context.Context, id string) (*Event, error) {
	op := writeOp{
		kind:      opReplay,
		id:        id,
		timestamp: time.Now().UTC(),
		resultCh:  make(chan *Event, 1),
		done:      make(chan error, 1),
	}
	select {
	case s.writeCh <- op:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case err := <-op.done:
		if err != nil {
			return nil, err
		}
		return <-op.resultCh, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// GetEvent fetches a single event by ID.
func (s *Store) GetEvent(ctx context.Context, id string) (*Event, error) {
	row := s.readDB.QueryRowContext(ctx, `
		SELECT id, event_type, target_url, payload, secret, idempotency_key,
		       status, attempts, max_retries, last_status_code, last_error,
		       dlq_reason, delivery_ms, created_at, updated_at, delivered_at, failed_at
		FROM events WHERE id = ?
	`, id)
	return scanEvent(row)
}

// ListDLQ queries events currently in the Dead Letter Queue.
func (s *Store) ListDLQ(ctx context.Context, limit, offset int) ([]Event, int, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}

	var total int
	if err := s.readDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE status = ?`, string(StatusDLQ)).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := s.readDB.QueryContext(ctx, `
		SELECT id, event_type, target_url, payload, secret, idempotency_key,
		       status, attempts, max_retries, last_status_code, last_error,
		       dlq_reason, delivery_ms, created_at, updated_at, delivered_at, failed_at
		FROM events
		WHERE status = ?
		ORDER BY updated_at DESC
		LIMIT ? OFFSET ?
	`, string(StatusDLQ), limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	items := make([]Event, 0)
	for rows.Next() {
		evt, err := scanEvent(rows)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, *evt)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

// CountDLQ returns the number of events currently in the DLQ.
func (s *Store) CountDLQ(ctx context.Context) (int64, error) {
	var count int64
	err := s.readDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE status = ?`, string(StatusDLQ)).Scan(&count)
	return count, err
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanEvent(scanner rowScanner) (*Event, error) {
	var e Event
	var payloadStr, statusStr string
	var createdStr, updatedStr string
	var deliveredStr, failedStr sql.NullString

	err := scanner.Scan(
		&e.ID, &e.EventType, &e.TargetURL, &payloadStr, &e.Secret, &e.IdempotencyKey,
		&statusStr, &e.Attempts, &e.MaxRetries, &e.LastStatusCode, &e.LastError,
		&e.DLQReason, &e.DeliveryMs, &createdStr, &updatedStr, &deliveredStr, &failedStr,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrEventNotFound
	}
	if err != nil {
		return nil, err
	}

	e.Payload = json.RawMessage(payloadStr)
	e.Status = EventStatus(statusStr)
	e.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdStr)
	e.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updatedStr)
	if deliveredStr.Valid && deliveredStr.String != "" {
		if t, err := time.Parse(time.RFC3339Nano, deliveredStr.String); err == nil {
			e.DeliveredAt = &t
		}
	}
	if failedStr.Valid && failedStr.String != "" {
		if t, err := time.Parse(time.RFC3339Nano, failedStr.String); err == nil {
			e.FailedAt = &t
		}
	}
	return &e, nil
}

// Close drains all pending write operations, checkpoints the WAL, and closes SQLite handles.
func (s *Store) Close() error {
	var closeErr error
	s.closeOnce.Do(func() {
		close(s.writeCh)
		s.wg.Wait()

		_ = s.stmtInsert.Close()
		_ = s.stmtDeliver.Close()
		_ = s.stmtRetry.Close()
		_ = s.stmtDLQ.Close()

		_, _ = s.writeDB.Exec("PRAGMA wal_checkpoint(TRUNCATE);")
		errRead := s.readDB.Close()
		errWrite := s.writeDB.Close()
		if errWrite != nil {
			closeErr = errWrite
		} else {
			closeErr = errRead
		}
	})
	return closeErr
}
