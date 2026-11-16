package outbox

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// fakeTx records INSERT statements issued by Append. Every other method is
// unused by Append and panics if called, so a silent interface change in
// Append fails the test loudly instead of passing vacuously.
type fakeTx struct {
	stmts []string
	args  [][]any
}

func (f *fakeTx) Exec(_ context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	f.stmts = append(f.stmts, sql)
	f.args = append(f.args, arguments)
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (f *fakeTx) Begin(context.Context) (pgx.Tx, error)                   { panic("unused") }
func (f *fakeTx) Commit(context.Context) error                            { return nil }
func (f *fakeTx) Rollback(context.Context) error                          { return nil }
func (f *fakeTx) Query(context.Context, string, ...any) (pgx.Rows, error) { panic("unused") }
func (f *fakeTx) QueryRow(context.Context, string, ...any) pgx.Row        { panic("unused") }
func (f *fakeTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	panic("unused")
}
func (f *fakeTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults { panic("unused") }
func (f *fakeTx) LargeObjects() pgx.LargeObjects                         { panic("unused") }
func (f *fakeTx) Conn() *pgx.Conn                                        { panic("unused") }
func (f *fakeTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	panic("unused")
}

// TestAppendValidatesBeforeExec pins the all-or-nothing validation of
// Append: an invalid event anywhere in the slice must fail the whole call
// before a single INSERT runs (the caller's transaction stays untouched).
func TestAppendValidatesBeforeExec(t *testing.T) {
	valid := NewEvent{EventType: "ok", AggregateID: "a", Payload: map[string]int{"n": 1}}
	cases := []struct {
		name  string
		event NewEvent
		want  string
	}{
		{"empty event type", NewEvent{AggregateID: "a"}, "EventType and AggregateID are required"},
		{"empty aggregate id", NewEvent{EventType: "t"}, "EventType and AggregateID are required"},
		{"unmarshalable payload", NewEvent{EventType: "t", AggregateID: "a", Payload: make(chan int)}, "marshal payload"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tx := &fakeTx{}
			err := Append(context.Background(), tx, valid, tc.event, valid)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
			if len(tx.stmts) != 0 {
				t.Fatalf("Append issued %d statements despite invalid input", len(tx.stmts))
			}
		})
	}
}

// TestAppendEmptyIsNoop checks the documented edge case: zero events must
// not touch the database at all.
func TestAppendEmptyIsNoop(t *testing.T) {
	tx := &fakeTx{}
	if err := Append(context.Background(), tx); err != nil {
		t.Fatalf("empty Append: %v", err)
	}
	if len(tx.stmts) != 0 {
		t.Fatalf("empty Append issued %d statements, want 0", len(tx.stmts))
	}
}

// TestAppendChunksLargeBatches pins the parameter-limit handling: a single
// INSERT binds 4 parameters per event and PostgreSQL's extended protocol
// caps statements at 65535, so Append must split batches at maxAppendEvents
// rows, restarting placeholder numbering in every chunk.
func TestAppendChunksLargeBatches(t *testing.T) {
	const n = 2*maxAppendEvents + 5
	events := make([]NewEvent, n)
	for i := range events {
		events[i] = NewEvent{EventType: "bulk", AggregateID: "agg", Payload: i}
	}

	tx := &fakeTx{}
	if err := Append(context.Background(), tx, events...); err != nil {
		t.Fatalf("append: %v", err)
	}

	wantChunks := []int{4 * maxAppendEvents, 4 * maxAppendEvents, 4 * 5}
	if len(tx.stmts) != len(wantChunks) {
		t.Fatalf("got %d statements, want %d", len(tx.stmts), len(wantChunks))
	}
	for i, args := range tx.args {
		if len(args) != wantChunks[i] {
			t.Fatalf("statement %d binds %d args, want %d", i, len(args), wantChunks[i])
		}
		if len(args) > 65535 {
			t.Fatalf("statement %d binds %d args, exceeding the 65535 protocol limit", i, len(args))
		}
		// Placeholder numbering must restart per chunk: ($1, $2, $3, $4) head.
		if head := "($1, $2, $3::jsonb, $4::jsonb)"; !strings.Contains(tx.stmts[i], head) {
			t.Fatalf("statement %d does not start placeholders at $1", i)
		}
	}

	// Chunk boundaries must not drop or duplicate events: first row of the
	// first chunk and first row of the tail chunk carry their own payloads
	// (an int payload marshals to a bare JSON number).
	if got, want := tx.args[0][2].(string), `0`; got != want {
		t.Fatalf("first chunk starts with payload %s, want %s", got, want)
	}
	wantTail := strconv.Itoa(2 * maxAppendEvents)
	if got := tx.args[2][2].(string); got != wantTail {
		t.Fatalf("tail chunk starts with payload %s, want %s", got, wantTail)
	}
}
