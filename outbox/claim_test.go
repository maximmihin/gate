package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

// newClaimRelay builds a Relay used only for direct claim calls (no Run),
// with the shared fast test timings.
func newClaimRelay(t *testing.T, dsn string, mut func(*Config)) *Relay {
	t.Helper()
	relay, err := NewRelay(baseConfig(t, dsn, newCollector(nil), mut))
	if err != nil {
		t.Fatalf("new relay: %v", err)
	}
	return relay
}

// decodeJSON decodes raw into a generic value, for comparing JSONB
// round-trips semantically (jsonb canonicalizes key order and whitespace).
func decodeJSON(t *testing.T, raw json.RawMessage) any {
	t.Helper()
	var got any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("payload %s is not valid JSON: %v", raw, err)
	}
	return got
}

// TestClaimRoundTrip pins the Append -> claim contract: headers and payload
// survive the JSONB round trip (struct via json.Marshal, json.RawMessage
// passed through, nil becoming JSON null), and the claim itself already
// counts as attempt 1.
func TestClaimRoundTrip(t *testing.T) {
	conn, dsn := setup(t)
	ctx := context.Background()

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	err = Append(ctx, tx,
		NewEvent{
			EventType:   "rt.struct",
			AggregateID: "agg-1",
			Payload: struct {
				N int `json:"n"`
			}{N: 42},
			Headers: map[string]string{"schema_version": "1", "trace": "abc"},
		},
		NewEvent{EventType: "rt.raw", AggregateID: "agg-2", Payload: json.RawMessage(`{"x":[1,2],"y":"s"}`)},
		NewEvent{EventType: "rt.nil", AggregateID: "agg-3"}, // Payload nil -> JSON null
	)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	relay := newClaimRelay(t, dsn, nil)
	events, err := relay.claim(ctx, conn)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("claimed %d events, want 3", len(events))
	}

	byType := map[string]Event{}
	for i, evt := range events {
		if evt.Attempts != 1 {
			t.Fatalf("event %d attempts = %d, want 1 (the claim itself)", i, evt.Attempts)
		}
		if evt.EventID == "" || len(evt.EventID) != 36 {
			t.Fatalf("event %d EventID = %q, want a UUID", i, evt.EventID)
		}
		if evt.CreatedAt.IsZero() || time.Since(evt.CreatedAt) > time.Minute {
			t.Fatalf("event %d CreatedAt = %v, want a recent timestamp", i, evt.CreatedAt)
		}
		byType[evt.EventType] = evt
	}

	// fmt prints maps with sorted keys, so Sprint comparison is stable.
	if got, want := fmt.Sprint(decodeJSON(t, byType["rt.struct"].Payload)), fmt.Sprint(map[string]any{"n": 42}); got != want {
		t.Fatalf("struct payload = %s, want %s", got, want)
	}
	if got, want := fmt.Sprint(decodeJSON(t, byType["rt.raw"].Payload)), fmt.Sprint(map[string]any{"x": []any{1, 2}, "y": "s"}); got != want {
		t.Fatalf("raw payload = %s, want %s", got, want)
	}
	if got := string(byType["rt.nil"].Payload); got != "null" {
		t.Fatalf("nil payload round-tripped as %s, want null", got)
	}

	wantHeaders := map[string]string{"schema_version": "1", "trace": "abc"}
	if got := byType["rt.struct"].Headers; len(got) != len(wantHeaders) || got["schema_version"] != "1" || got["trace"] != "abc" {
		t.Fatalf("headers = %v, want %v", got, wantHeaders)
	}
	if got := byType["rt.raw"].Headers; len(got) != 0 {
		t.Fatalf("nil headers round-tripped as %v, want empty map", got)
	}
}

// TestClaimPreScheduleSurvivesCrash simulates the "crash after claim,
// before Publish" row of the README failure table by claiming directly and
// never publishing. The claim commit must have incremented attempts and
// pushed next_attempt_at into the future, so the pending rows are invisible
// to the next claim until the retry delay elapses (fast-forwarded here by
// rewinding next_attempt_at instead of sleeping). It also covers a batch of
// events sharing one next_attempt_at: they are claimed together in id order.
func TestClaimPreScheduleSurvivesCrash(t *testing.T) {
	conn, dsn := setup(t)
	ctx := context.Background()

	appendN(t, conn, "crash", 3) // all due now, identical next_attempt_at

	// A generous base delay keeps the "not re-claimable yet" window robust
	// against client-side scheduler pauses between claim and the checks.
	relay := newClaimRelay(t, dsn, func(cfg *Config) {
		cfg.RetryBaseDelay = 500 * time.Millisecond
		cfg.RetryMaxDelay = time.Second
	})

	first, err := relay.claim(ctx, conn)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if len(first) != 3 {
		t.Fatalf("first claim got %d events, want 3", len(first))
	}
	for i, evt := range first {
		if i > 0 && evt.ID <= first[i-1].ID {
			t.Fatalf("claim returned ids out of order: %d after %d", evt.ID, first[i-1].ID)
		}
		if evt.Attempts != 1 {
			t.Fatalf("event id=%d attempts = %d, want 1", evt.ID, evt.Attempts)
		}
	}

	// Durability of the pre-schedule: attempts persisted, retry in the
	// future, nothing due for a re-claim (the relay "died" after claiming).
	var rows, withAttempts, dueNow int
	var minNext, maxNext time.Time
	if err := conn.QueryRow(ctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE attempts = 1),
		       count(*) FILTER (WHERE next_attempt_at <= now()),
		       min(next_attempt_at), max(next_attempt_at)
		FROM outbox`).Scan(&rows, &withAttempts, &dueNow, &minNext, &maxNext); err != nil {
		t.Fatalf("state after claim: %v", err)
	}
	if rows != 3 || withAttempts != 3 || dueNow != 0 {
		t.Fatalf("after claim: rows=%d attempts=1:%d due-now=%d, want 3/3/0", rows, withAttempts, dueNow)
	}
	now := time.Now()
	if !minNext.After(now) || maxNext.Sub(now) > 2*time.Second {
		t.Fatalf("next_attempt_at window [%v, %v] not within (now, now+2s)", minNext, maxNext)
	}

	// The next relay instance must not see the crashed batch.
	second, err := relay.claim(ctx, conn)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if len(second) != 0 {
		t.Fatalf("second claim re-picked %d events before next_attempt_at", len(second))
	}

	// Simulate the retry delay elapsing.
	if _, err := conn.Exec(ctx, "UPDATE outbox SET next_attempt_at = now()"); err != nil {
		t.Fatalf("fast-forward: %v", err)
	}
	third, err := relay.claim(ctx, conn)
	if err != nil {
		t.Fatalf("third claim: %v", err)
	}
	if len(third) != 3 {
		t.Fatalf("third claim got %d events, want 3", len(third))
	}
	for _, evt := range third {
		if evt.Attempts != 2 {
			t.Fatalf("event id=%d attempts = %d after re-claim, want 2", evt.ID, evt.Attempts)
		}
	}
}

// TestClaimEmptyTable pins the claim's behavior on an empty outbox: no
// error, no events, and the transaction is closed cleanly (the connection
// stays usable for the next claim).
func TestClaimEmptyTable(t *testing.T) {
	conn, dsn := setup(t)

	relay := newClaimRelay(t, dsn, nil)
	for i := 0; i < 2; i++ {
		events, err := relay.claim(context.Background(), conn)
		if err != nil {
			t.Fatalf("claim %d on empty table: %v", i, err)
		}
		if len(events) != 0 {
			t.Fatalf("claim %d on empty table returned %d events", i, len(events))
		}
	}
}

// slowPublisher blocks until its context is done (simulating a hung broker
// client), then fails; from the second call on it succeeds immediately. It
// records the remaining deadline of every call.
type slowPublisher struct {
	mu        sync.Mutex
	remaining []time.Duration
}

func (p *slowPublisher) Publish(ctx context.Context, _ Event) error {
	deadline, _ := ctx.Deadline()
	p.mu.Lock()
	p.remaining = append(p.remaining, time.Until(deadline))
	call := len(p.remaining)
	p.mu.Unlock()
	if call == 1 {
		<-ctx.Done() // ignore the broker until the relay gives up
		return ctx.Err()
	}
	return nil
}

// TestPublishTimeoutRetries checks that a publisher hanging past
// PublishTimeout gets its context deadline, the event is scheduled for a
// retry instead of blocking the relay, and the retry goes through cleanly
// (attempts = 2, processed, last_error cleared).
func TestPublishTimeoutRetries(t *testing.T) {
	conn, dsn := setup(t)
	ctx := context.Background()

	appendN(t, conn, "slow", 1)

	pub := &slowPublisher{}
	relay, _ := startRelay(t, dsn, pub, func(cfg *Config) {
		cfg.PublishTimeout = 150 * time.Millisecond
	})

	// Wait on the durable DB outcome, not on relay counters: Published is
	// incremented before the mark-processed commit.
	var attempts int
	var processed, lastError *string
	waitFor(t, 5*time.Second, "event published on retry", func() bool {
		return conn.QueryRow(ctx,
			`SELECT attempts, processed_at::text, last_error FROM outbox WHERE event_type = 'slow'`,
		).Scan(&attempts, &processed, &lastError) == nil && attempts == 2 && processed != nil && lastError == nil
	})

	if s := relay.Stats(); s.Failed != 1 || s.Published != 1 {
		t.Fatalf("relay stats failed=%d published=%d, want 1/1", s.Failed, s.Published)
	}

	pub.mu.Lock()
	remaining := append([]time.Duration(nil), pub.remaining...)
	pub.mu.Unlock()
	if len(remaining) != 2 {
		t.Fatalf("publisher called %d times, want 2", len(remaining))
	}
	// First call must have carried a real deadline of ~PublishTimeout (the
	// bounds are loose to stay stable under -race and load).
	if remaining[0] < 100*time.Millisecond || remaining[0] > 200*time.Millisecond {
		t.Fatalf("first publish deadline remaining = %v, want ~150ms", remaining[0])
	}
}

// TestTwoRelaysDifferentLockKeys models two outbox deployments sharing one
// database: relays with different AdvisoryLockKeys must both hold
// leadership at the same time, split the events between them, and never
// deliver the same event twice.
func TestTwoRelaysDifferentLockKeys(t *testing.T) {
	conn, dsn := setup(t)

	colA := newCollector(nil)
	relayA, _ := startRelay(t, dsn, colA, nil)
	waitFor(t, 5*time.Second, "relay A leads", func() bool {
		return relayA.Stats().LeadershipGains == 1
	})

	colB := newCollector(nil)
	relayB, _ := startRelay(t, dsn, colB, func(cfg *Config) {
		cfg.AdvisoryLockKey = DefaultAdvisoryLockKey + 1
	})
	waitFor(t, 5*time.Second, "relay B leads too", func() bool {
		return relayB.Stats().LeadershipGains == 1
	})

	// B must not have stolen A's leadership: distinct keys cannot contend.
	if got := relayA.Stats().LeadershipGains; got != 1 {
		t.Fatalf("relay A leadership gains = %d after B started, want 1", got)
	}

	appendN(t, conn, "shared", 6)
	waitFor(t, 5*time.Second, "all events delivered across both relays", func() bool {
		return len(colA.snapshot())+len(colB.snapshot()) == 6
	})

	seen := map[int64]bool{}
	for _, evt := range append(colA.snapshot(), colB.snapshot()...) {
		if seen[evt.ID] {
			t.Fatalf("event %d delivered by both relays", evt.ID)
		}
		seen[evt.ID] = true
	}
	if len(seen) != 6 {
		t.Fatalf("distinct delivered events = %d, want 6", len(seen))
	}
}
