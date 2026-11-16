// Command outbox-demo demonstrates the transactional outbox end to end:
//
//   - a producer committing business transactions with outbox events
//     (plus one poison event that always fails to publish);
//   - two relay instances against the same database: "primary" holds the
//     advisory lock, "standby" waits; at t=6s the primary is killed and the
//     standby takes over without losing or duplicating events;
//   - the poison event exhausts its attempts and is dead-lettered, and the
//     stream keeps flowing (liveness over head-of-line blocking).
//
// Start PostgreSQL first: docker compose up -d --wait
// Then: go run ./cmd/outbox-demo
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/maximmihin/gate/outbox"
)

// printer is a Publisher that logs healthy events and permanently fails
// "demo.poison" to exercise the retry/dead-letter path.
type printer struct{ logger *slog.Logger }

func (p printer) Publish(_ context.Context, evt outbox.Event) error {
	if evt.EventType == "demo.poison" {
		return errors.New("publisher: simulated permanent failure for demo.poison")
	}
	p.logger.Info("PUBLISHED",
		"id", evt.ID, "event_id", evt.EventID, "type", evt.EventType,
		"aggregate", evt.AggregateID, "payload", string(evt.Payload))
	return nil
}

func main() {
	dsn := flag.String("dsn", "postgres://outbox:outbox@localhost:5422/outbox?sslmode=disable", "postgres DSN")
	duration := flag.Duration("for", 15*time.Second, "how long to run the demo")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *duration)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	conn, err := pgx.Connect(ctx, *dsn)
	if err != nil {
		fatal(logger, err)
	}
	if err := outbox.Migrate(ctx, conn); err != nil {
		fatal(logger, err)
	}
	// Start each demo run with a clean log.
	if _, err := conn.Exec(ctx, "TRUNCATE outbox RESTART IDENTITY"); err != nil {
		fatal(logger, err)
	}
	_ = conn.Close(ctx)

	cc, err := pgx.ParseConfig(*dsn)
	if err != nil {
		fatal(logger, err)
	}

	var wg sync.WaitGroup
	startRelay := func(name string, rctx context.Context) *outbox.Relay {
		relay, err := outbox.NewRelay(outbox.Config{
			ConnConfig:     cc,
			Publisher:      printer{logger: logger.With("relay", name)},
			Logger:         logger.With("relay", name),
			MaxAttempts:    3,
			RetryBaseDelay: 200 * time.Millisecond,
			RetryMaxDelay:  1 * time.Second,
			PollInterval:   500 * time.Millisecond,
			LockRetry:      200 * time.Millisecond,
			OnDead: func(evt outbox.Event, err error) {
				logger.Error("ALERT: event dead-lettered, operator action required",
					"id", evt.ID, "event_id", evt.EventID, "type", evt.EventType, "error", err)
			},
		})
		if err != nil {
			fatal(logger, err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := relay.Run(rctx); err != nil &&
				!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				logger.Error("relay stopped unexpectedly", "error", err)
			}
		}()
		return relay
	}

	primaryCtx, cancelPrimary := context.WithCancel(ctx)
	primary := startRelay("primary", primaryCtx)

	// Determinism: wait until the primary actually holds leadership before
	// starting the standby, otherwise the two race for the lock and the
	// failover below might demonstrate nothing.
	deadline := time.Now().Add(5 * time.Second)
	for primary.Stats().LeadershipGains == 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if primary.Stats().LeadershipGains == 0 {
		fatal(logger, errors.New("primary relay did not acquire leadership"))
	}
	standby := startRelay("standby", ctx)

	go func() {
		select {
		case <-time.After(6 * time.Second):
			logger.Info("demo: killing primary relay; standby must take over")
			cancelPrimary()
		case <-ctx.Done():
		}
	}()

	// Producer: one business transaction every 400ms, each appending an
	// outbox event in the same transaction.
	prodConn, err := pgx.Connect(ctx, *dsn)
	if err != nil {
		fatal(logger, err)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer prodConn.Close(context.Background())
		ticker := time.NewTicker(400 * time.Millisecond)
		defer ticker.Stop()
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			tx, err := prodConn.Begin(ctx)
			if err != nil {
				logger.Error("producer: begin", "error", err)
				continue
			}
			// ... here the producer would also write its business rows
			// in the same transaction ...
			events := []outbox.NewEvent{{
				EventType:   "order.touched",
				AggregateID: fmt.Sprintf("order-%d", i%3),
				Payload:     map[string]any{"n": i},
				Headers:     map[string]string{"correlation_id": fmt.Sprintf("req-%d", i)},
			}}
			if i == 2 {
				events = append(events, outbox.NewEvent{
					EventType:   "demo.poison",
					AggregateID: "poison",
					Payload:     map[string]any{"why": "demonstrates dead-lettering"},
				})
			}
			if err := outbox.Append(ctx, tx, events...); err != nil {
				logger.Error("producer: append", "error", err)
				_ = tx.Rollback(ctx)
				continue
			}
			if err := tx.Commit(ctx); err != nil {
				logger.Error("producer: commit", "error", err)
			}
		}
	}()

	<-ctx.Done()
	logger.Info("demo: shutting down")
	wg.Wait()

	for name, relay := range map[string]*outbox.Relay{"primary": primary, "standby": standby} {
		s := relay.Stats()
		logger.Info("relay stats", "relay", name,
			"published", s.Published, "failed", s.Failed,
			"dead", s.Dead, "leadership_gains", s.LeadershipGains)
	}
}

func fatal(logger *slog.Logger, err error) {
	logger.Error("demo failed", "error", err)
	os.Exit(1)
}
