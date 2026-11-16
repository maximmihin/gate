// Package outbox implements the transactional outbox pattern for PostgreSQL.
//
// Write side: Append inserts rows into the outbox table inside the same
// database transaction as the business write. If the transaction rolls back,
// the events never existed. This solves the dual-write problem (DDIA 2e,
// ch. 12, "Keeping Systems in Sync"): the database is the single leader that
// fixes the order of events.
//
// Read side: a Relay claims pending rows with SELECT ... FOR UPDATE SKIP
// LOCKED and hands them to a Publisher (typically a Kafka producer). Exactly
// one relay instance at a time is active, elected via a session-level
// pg_advisory_lock on a dedicated connection. LISTEN/NOTIFY wakes the relay
// within milliseconds after commit; a periodic poll is the safety net for
// missed notifications, retries and events that accumulated while no relay
// held leadership.
//
// Delivery semantics are at-least-once. Consumers MUST deduplicate by
// Event.EventID (DDIA 2e, ch. 13, "Uniquely identifying requests"; see
// README.md for the consumer-side snippet). Per-aggregate ordering is
// preserved in the happy path; a failed publish schedules a later retry and
// the relay moves on, so events of the same aggregate can overtake each
// other during failures (DDIA 2e, ch. 12, Figure 12-2). Consumers that care
// should carry and check a per-aggregate version.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Event is a single message read from the outbox table.
type Event struct {
	// ID is the monotonic log position: batched, ordered claims use it as
	// the cursor (analogous to a consumer offset, DDIA 2e, ch. 12).
	ID int64
	// EventID is a stable UUID assigned at insert time. Consumers
	// deduplicate on this value.
	EventID     string
	EventType   string
	AggregateID string // ordering key; use as the broker partition key
	Payload     json.RawMessage
	Headers     map[string]string
	// Attempts includes the current delivery attempt (>= 1 once claimed).
	Attempts  int
	CreatedAt time.Time
}

// NewEvent is an event to append alongside a business write.
type NewEvent struct {
	EventType   string
	AggregateID string
	// Payload is marshalled to JSON. json.RawMessage and []byte are passed
	// through as-is; anything else goes through json.Marshal. Nil becomes
	// JSON null, which is a valid jsonb value.
	Payload any
	// Headers is optional string-to-string metadata (correlation ids,
	// schema version, ...). Nil becomes an empty object.
	Headers map[string]string
}

// maxAppendEvents bounds the rows per INSERT statement. Each row binds 4
// parameters and PostgreSQL's extended protocol caps a statement at 65535
// of them, so an unchunked Append would fail at ~16k events with a confusing
// protocol error. All chunks share the caller's transaction, so atomicity
// is unaffected.
const maxAppendEvents = 16000

// appendRow is a NewEvent validated and marshalled for INSERT.
type appendRow struct {
	eventType   string
	aggregateID string
	payload     string // JSON text for the jsonb column
	headers     string // JSON object text for the jsonb column
}

// Append inserts events into the outbox table using the caller's
// transaction. Committing or rolling back that transaction is the caller's
// job: Append only ever runs inside it, which is what makes the write
// atomic with the business change. All events are validated and marshalled
// before the first statement runs, so an invalid event inserts nothing.
func Append(ctx context.Context, tx pgx.Tx, events ...NewEvent) error {
	if len(events) == 0 {
		return nil
	}

	rows := make([]appendRow, len(events))
	for i, e := range events {
		if e.EventType == "" || e.AggregateID == "" {
			return fmt.Errorf("outbox: event %d: EventType and AggregateID are required", i)
		}
		payload, err := marshalPayload(e.Payload)
		if err != nil {
			return fmt.Errorf("outbox: event %d: marshal payload: %w", i, err)
		}
		headers := e.Headers
		if headers == nil {
			headers = map[string]string{}
		}
		headerJSON, err := json.Marshal(headers)
		if err != nil {
			return fmt.Errorf("outbox: event %d: marshal headers: %w", i, err)
		}
		rows[i] = appendRow{
			eventType:   e.EventType,
			aggregateID: e.AggregateID,
			payload:     string(payload),
			headers:     string(headerJSON),
		}
	}

	for start := 0; start < len(rows); start += maxAppendEvents {
		end := min(start+maxAppendEvents, len(rows))
		if err := appendRows(ctx, tx, rows[start:end]); err != nil {
			return err
		}
	}
	return nil
}

// appendRows emits one multi-row INSERT for a chunk of pre-marshalled events.
func appendRows(ctx context.Context, tx pgx.Tx, rows []appendRow) error {
	var sb strings.Builder
	sb.WriteString("INSERT INTO outbox (event_type, aggregate_id, payload, headers) VALUES ")
	args := make([]any, 0, len(rows)*4)

	for i, r := range rows {
		if i > 0 {
			sb.WriteString(", ")
		}
		n := len(args)
		fmt.Fprintf(&sb, "($%d, $%d, $%d::jsonb, $%d::jsonb)", n+1, n+2, n+3, n+4)
		args = append(args, r.eventType, r.aggregateID, r.payload, r.headers)
	}

	_, err := tx.Exec(ctx, sb.String(), args...)
	return err
}

func marshalPayload(p any) (json.RawMessage, error) {
	switch v := p.(type) {
	case nil:
		return json.RawMessage("null"), nil
	case json.RawMessage:
		return v, nil
	case []byte:
		return json.RawMessage(v), nil
	default:
		b, err := json.Marshal(p)
		if err != nil {
			return nil, err
		}
		return b, nil
	}
}
