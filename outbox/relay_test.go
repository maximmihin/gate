package outbox

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("OUTBOX_TEST_DSN")
	if dsn == "" {
		t.Skip("set OUTBOX_TEST_DSN (e.g. postgres://outbox:outbox@localhost:5422/outbox?sslmode=disable) to run integration tests")
	}
	return dsn
}

var migrateOnce sync.Once

// setup connects, ensures the schema exists and empties the outbox table.
func setup(t *testing.T) (*pgx.Conn, string) {
	t.Helper()
	dsn := testDSN(t)
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	migrateOnce.Do(func() {
		if err := Migrate(ctx, conn); err != nil {
			t.Fatalf("migrate: %v", err)
		}
	})
	if _, err := conn.Exec(ctx, "TRUNCATE outbox RESTART IDENTITY"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn, dsn
}

// collector is a Publisher that records delivered events and can simulate
// failures per event type.
type collector struct {
	mu     sync.Mutex
	events []Event
	failN  map[string]int // event_type -> number of remaining failures
}

func newCollector(failN map[string]int) *collector {
	if failN == nil {
		failN = map[string]int{}
	}
	return &collector{failN: failN}
}

func (c *collector) Publish(_ context.Context, evt Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failN[evt.EventType] > 0 {
		c.failN[evt.EventType]--
		return errors.New("simulated publish failure")
	}
	c.events = append(c.events, evt)
	return nil
}

func (c *collector) snapshot() []Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Event(nil), c.events...)
}

func startRelay(t *testing.T, dsn string, pub Publisher, mut func(*Config)) (*Relay, context.CancelFunc) {
	t.Helper()
	cc, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	cfg := Config{
		ConnConfig:     cc,
		Publisher:      pub,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		BatchSize:      10,
		PollInterval:   100 * time.Millisecond,
		LockRetry:      50 * time.Millisecond,
		MaxAttempts:    3,
		RetryBaseDelay: 20 * time.Millisecond,
		RetryMaxDelay:  100 * time.Millisecond,
		QueryTimeout:   5 * time.Second,
		PublishTimeout: 5 * time.Second,
	}
	if mut != nil {
		mut(&cfg)
	}
	relay, err := NewRelay(cfg)
	if err != nil {
		t.Fatalf("new relay: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = relay.Run(ctx) }()
	return relay, cancel
}

func waitFor(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", desc)
}

func appendN(t *testing.T, conn *pgx.Conn, eventType string, n int) {
	t.Helper()
	ctx := context.Background()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	evts := make([]NewEvent, n)
	for i := range evts {
		evts[i] = NewEvent{
			EventType:   eventType,
			AggregateID: "agg-1",
			Payload:     map[string]any{"i": i},
		}
	}
	if err := Append(ctx, tx, evts...); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func TestAppendRollbackAndCommit(t *testing.T) {
	conn, _ := setup(t)
	ctx := context.Background()

	// Commit keeps the event.
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := Append(ctx, tx, NewEvent{EventType: "kept", AggregateID: "a", Payload: map[string]any{"k": 1}}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Rollback discards it as if it never existed.
	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := Append(ctx, tx, NewEvent{EventType: "discarded", AggregateID: "a", Payload: nil}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	var n int
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM outbox").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("want 1 row after rollback, got %d", n)
	}
}

func TestNotifyOnlyOnCommit(t *testing.T) {
	conn, dsn := setup(t)
	ctx := context.Background()

	listener, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect listener: %v", err)
	}
	defer listener.Close(context.Background())
	if _, err := listener.Exec(ctx, "LISTEN "+DefaultChannel); err != nil {
		t.Fatalf("listen: %v", err)
	}

	// Commit must produce a notification.
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := Append(ctx, tx, NewEvent{EventType: "n", AggregateID: "a"}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	nctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	notif, err := listener.WaitForNotification(nctx)
	if err != nil {
		t.Fatalf("expected notification after commit: %v", err)
	}
	if notif.Channel != DefaultChannel {
		t.Fatalf("notification channel = %q, want %q", notif.Channel, DefaultChannel)
	}

	// Rollback must not.
	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := Append(ctx, tx, NewEvent{EventType: "n", AggregateID: "a"}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	nctx2, cancel2 := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel2()
	if _, err := listener.WaitForNotification(nctx2); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected no notification after rollback, got err=%v", err)
	}
}

// TestRetryDelayMath pins the retry policy semantics: attempt 1 waits the
// base delay, each next attempt doubles it, RetryMaxDelay caps the growth,
// and every draw is jittered by at most RetryJitter (wait.Jitter draws
// uniformly from [d, d*(1+jitter)]).
func TestRetryDelayMath(t *testing.T) {
	relay, err := NewRelay(Config{
		ConnConfig:     &pgx.ConnConfig{},
		Publisher:      newCollector(nil),
		RetryBaseDelay: 20 * time.Millisecond,
		RetryMaxDelay:  100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new relay: %v", err)
	}

	for attempts, want := range map[int]time.Duration{
		1:  20 * time.Millisecond,
		2:  40 * time.Millisecond,
		3:  80 * time.Millisecond,
		4:  100 * time.Millisecond, // 160ms capped
		99: 100 * time.Millisecond,
	} {
		for i := 0; i < 50; i++ {
			got := relay.retryDelay(attempts)
			hi := want + time.Duration(float64(want)*relay.cfg.RetryJitter)
			if got < want || got > hi {
				t.Fatalf("retryDelay(%d) = %v, want within [%v, %v]", attempts, got, want, hi)
			}
		}
	}

	// Jitter must be drawn per call, so a failed batch does not retry as a
	// synchronized herd.
	var distinct bool
	for i := 0; i < 100 && !distinct; i++ {
		distinct = relay.retryDelay(1) != relay.retryDelay(1)
	}
	if !distinct {
		t.Fatal("retryDelay draws identical jitter for every row")
	}
}

func TestRelayDeliversInOrder(t *testing.T) {
	conn, dsn := setup(t)

	for i := 0; i < 3; i++ {
		appendN(t, conn, "e", 4)
	}

	col := newCollector(nil)
	startRelay(t, dsn, col, nil)

	waitFor(t, 5*time.Second, "all 12 events delivered", func() bool {
		return len(col.snapshot()) == 12
	})

	var last int64
	for _, evt := range col.snapshot() {
		if evt.ID <= last {
			t.Fatalf("events delivered out of order: %d after %d", evt.ID, last)
		}
		last = evt.ID
	}

	ctx := context.Background()
	var processed, maxAttempts int
	if err := conn.QueryRow(ctx,
		"SELECT count(*) FILTER (WHERE processed_at IS NOT NULL), coalesce(max(attempts), 0) FROM outbox",
	).Scan(&processed, &maxAttempts); err != nil {
		t.Fatalf("query outcomes: %v", err)
	}
	if processed != 12 {
		t.Fatalf("processed = %d, want 12", processed)
	}
	if maxAttempts != 1 {
		t.Fatalf("max attempts = %d, want 1 on the happy path", maxAttempts)
	}
}

func TestRetryThenDeadLetter(t *testing.T) {
	conn, dsn := setup(t)

	appendN(t, conn, "ok", 3)
	appendN(t, conn, "flaky", 1)  // fails twice, succeeds on 3rd attempt
	appendN(t, conn, "poison", 1) // always fails -> dead after MaxAttempts

	dead := make(chan Event, 1)
	col := newCollector(map[string]int{"flaky": 2, "poison": 1000})
	startRelay(t, dsn, col, func(cfg *Config) {
		cfg.OnDead = func(evt Event, _ error) { dead <- evt }
	})

	// Everything except the poison event eventually goes through.
	waitFor(t, 5*time.Second, "ok + flaky delivered", func() bool {
		return len(col.snapshot()) == 4
	})

	// The flaky event needed exactly 3 attempts (MaxAttempts).
	ctx := context.Background()
	var flakyAttempts int
	waitFor(t, 5*time.Second, "flaky attempts recorded", func() bool {
		return conn.QueryRow(ctx,
			"SELECT attempts FROM outbox WHERE event_type = 'flaky'").Scan(&flakyAttempts) == nil &&
			flakyAttempts == 3
	})

	// The poison event is dead-lettered, never published, and the relay
	// keeps working (the 3 ok events prove liveness).
	select {
	case evt := <-dead:
		if evt.EventType != "poison" {
			t.Fatalf("dead-lettered event = %q, want poison", evt.EventType)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for dead-letter hook")
	}
	var deadCount int
	if err := conn.QueryRow(ctx,
		"SELECT count(*) FROM outbox WHERE dead_at IS NOT NULL").Scan(&deadCount); err != nil {
		t.Fatalf("dead count: %v", err)
	}
	if deadCount != 1 {
		t.Fatalf("dead rows = %d, want 1", deadCount)
	}
}

func TestFailoverSingleLeader(t *testing.T) {
	conn, dsn := setup(t)

	appendN(t, conn, "e", 3)

	colA := newCollector(nil)
	relayA, cancelA := startRelay(t, dsn, colA, nil)
	waitFor(t, 5*time.Second, "primary delivered first 3", func() bool {
		return len(colA.snapshot()) == 3
	})

	// Start a second relay: it must stay standby while A holds the lock.
	colB := newCollector(nil)
	relayB, _ := startRelay(t, dsn, colB, nil)
	time.Sleep(300 * time.Millisecond)
	if got := relayB.Stats().LeadershipGains; got != 0 {
		t.Fatalf("standby acquired leadership while primary was alive (gains=%d)", got)
	}
	if n := len(colB.snapshot()); n != 0 {
		t.Fatalf("standby published %d events while primary was alive", n)
	}

	// Kill the primary; the standby must take over and deliver the rest.
	cancelA()
	_ = relayA
	appendN(t, conn, "e", 4)
	waitFor(t, 5*time.Second, "standby delivered the rest", func() bool {
		return len(colB.snapshot()) == 4
	})

	// No event was published twice across the two relays.
	seen := map[int64]bool{}
	for _, evt := range append(colA.snapshot(), colB.snapshot()...) {
		if seen[evt.ID] {
			t.Fatalf("event %d published by both relays", evt.ID)
		}
		seen[evt.ID] = true
	}
	if len(seen) != 7 {
		t.Fatalf("distinct published events = %d, want 7", len(seen))
	}
}
