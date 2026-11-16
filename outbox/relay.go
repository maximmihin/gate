package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"regexp"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	// DefaultChannel must match the channel used by the notify trigger in
	// migrations/0001_outbox.up.sql.
	DefaultChannel = "outbox_inserted"

	// DefaultAdvisoryLockKey spells "outbox" in hex. All relay instances
	// competing for the same outbox table must agree on it; separate outbox
	// deployments sharing a database must pick different keys.
	DefaultAdvisoryLockKey = 0x6F7574626F78

	DefaultBatchSize      = 100
	DefaultPollInterval   = 5 * time.Second
	DefaultLockRetry      = time.Second
	DefaultMaxAttempts    = 8
	DefaultRetryBaseDelay = time.Second
	DefaultRetryMaxDelay  = time.Minute
	DefaultQueryTimeout   = 30 * time.Second
	DefaultPublishTimeout = 30 * time.Second
)

// Config configures a Relay.
type Config struct {
	// ConnConfig is used to open the relay's two dedicated connections:
	// a work connection (advisory lock + all queries) and a notify
	// connection (LISTEN, otherwise idle). NewRelay may fill in defaults
	// for application_name, statement_timeout and DialFunc (TCP keepalive)
	// if the caller has not set them. Do not share one ConnConfig-derived
	// pool with application code: the relay deliberately avoids pgxpool.
	ConnConfig *pgx.ConnConfig

	// Channel is the LISTEN/NOTIFY channel name (default DefaultChannel).
	Channel string

	// AdvisoryLockKey is the leader-election key (default
	// DefaultAdvisoryLockKey). See DefaultAdvisoryLockKey.
	AdvisoryLockKey int64

	BatchSize      int           // rows per claim (default 100)
	PollInterval   time.Duration // fallback poll: retries, missed notifications (default 5s)
	LockRetry      time.Duration // standby lock retry interval (default 1s)
	MaxAttempts    int           // delivery attempts before dead-lettering (default 8)
	RetryBaseDelay time.Duration // exponential backoff base (default 1s)
	RetryMaxDelay  time.Duration // exponential backoff cap (default 1m)
	QueryTimeout   time.Duration // client-side timeout for outbox queries (default 30s)
	PublishTimeout time.Duration // per-event timeout passed to Publisher (default 30s)

	// Publisher receives claimed events, one call at a time, in id order
	// within a batch. Returning nil means "durably accepted downstream".
	Publisher Publisher

	Logger *slog.Logger

	// OnDead, if set, is invoked once for every event that is
	// dead-lettered. Use it to page an operator: a dead event needs
	// human intervention (fix payload, fix code, or drop it).
	OnDead func(evt Event, err error)
}

var channelRe = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// Relay delivers outbox events to a Publisher.
//
// Run it as one goroutine per process. Across processes, exactly one relay
// holds the session advisory lock and publishes at any moment: single
// writer, total order of delivered events (DDIA 2e, ch. 12, "the situation
// would be better if there really was only one leader"). If the leader dies,
// its PostgreSQL session ends and the lock is released; a standby takes over
// within LockRetry.
type Relay struct {
	cfg Config
	log *slog.Logger
	pub Publisher

	published   atomic.Uint64
	failed      atomic.Uint64
	dead        atomic.Uint64
	leaderships atomic.Uint64
}

// Stats is a point-in-time snapshot of relay counters.
type Stats struct {
	Published       uint64
	Failed          uint64
	Dead            uint64
	LeadershipGains uint64
}

// Stats returns a snapshot of the relay's counters.
func (r *Relay) Stats() Stats {
	return Stats{
		Published:       r.published.Load(),
		Failed:          r.failed.Load(),
		Dead:            r.dead.Load(),
		LeadershipGains: r.leaderships.Load(),
	}
}

// NewRelay validates the configuration and applies defaults.
func NewRelay(cfg Config) (*Relay, error) {
	if cfg.ConnConfig == nil {
		return nil, errors.New("outbox: Config.ConnConfig is required")
	}
	if cfg.Publisher == nil {
		return nil, errors.New("outbox: Config.Publisher is required")
	}
	if cfg.Channel == "" {
		cfg.Channel = DefaultChannel
	}
	if !channelRe.MatchString(cfg.Channel) {
		return nil, fmt.Errorf("outbox: invalid channel name %q: must match [a-z_][a-z0-9_]*", cfg.Channel)
	}
	if cfg.AdvisoryLockKey == 0 {
		cfg.AdvisoryLockKey = DefaultAdvisoryLockKey
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = DefaultBatchSize
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = DefaultPollInterval
	}
	if cfg.LockRetry <= 0 {
		cfg.LockRetry = DefaultLockRetry
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = DefaultMaxAttempts
	}
	if cfg.RetryBaseDelay <= 0 {
		cfg.RetryBaseDelay = DefaultRetryBaseDelay
	}
	if cfg.RetryMaxDelay <= 0 {
		cfg.RetryMaxDelay = DefaultRetryMaxDelay
	}
	if cfg.QueryTimeout <= 0 {
		cfg.QueryTimeout = DefaultQueryTimeout
	}
	if cfg.PublishTimeout <= 0 {
		cfg.PublishTimeout = DefaultPublishTimeout
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	// Session defaults for the relay's dedicated connections. TCP keepalive
	// matters: it is how a dead leader's session (and thus its advisory
	// lock) gets reaped promptly so a standby can take over.
	cc := cfg.ConnConfig
	if cc.RuntimeParams == nil {
		cc.RuntimeParams = map[string]string{}
	}
	if _, ok := cc.RuntimeParams["application_name"]; !ok {
		cc.RuntimeParams["application_name"] = "outbox-relay"
	}
	if _, ok := cc.RuntimeParams["statement_timeout"]; !ok {
		cc.RuntimeParams["statement_timeout"] = cfg.QueryTimeout.String()
	}
	if cc.DialFunc == nil {
		dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
		cc.DialFunc = dialer.DialContext
	}

	return &Relay{cfg: cfg, log: cfg.Logger, pub: cfg.Publisher}, nil
}

// Run blocks until ctx is cancelled, maintaining connections, leadership and
// delivery. It reconnects with backoff when a connection dies; on graceful
// shutdown it closes both connections, which releases the advisory lock and
// lets a standby take over quickly. It always returns a non-nil error
// (context.Canceled or context.DeadlineExceeded on shutdown).
func (r *Relay) Run(ctx context.Context) error {
	const maxBackoff = 30 * time.Second
	backoff := time.Second
	for {
		start := time.Now()
		err := r.runOnce(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Since(start) >= maxBackoff {
			backoff = time.Second // the previous session was healthy; reset
		}
		r.log.Warn("outbox relay: connection lost, reconnecting", "error", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// runOnce opens both connections, acquires leadership and runs the delivery
// loop until ctx is cancelled or a connection error forces a reconnect.
func (r *Relay) runOnce(ctx context.Context) error {
	work, err := pgx.ConnectConfig(ctx, r.cfg.ConnConfig)
	if err != nil {
		return fmt.Errorf("dial work connection: %w", err)
	}
	defer closeConn(work)

	notify, err := pgx.ConnectConfig(ctx, r.cfg.ConnConfig)
	if err != nil {
		return fmt.Errorf("dial notify connection: %w", err)
	}
	defer closeConn(notify)

	gctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wake := make(chan struct{}, 1)
	listenErr := make(chan error, 1)
	go func() { listenErr <- r.listen(gctx, notify, wake) }()

	if err := r.acquireLock(gctx, work, listenErr); err != nil {
		return err
	}
	r.leaderships.Add(1)
	r.log.Info("outbox relay: leadership acquired",
		"advisory_key", r.cfg.AdvisoryLockKey, "pid", work.PgConn().PID())
	defer r.releaseLock(work)

	// Drain whatever accumulated before we were the leader.
	nudge(wake)

	for {
		if err := r.drain(gctx, work); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake: // triggered by LISTEN/NOTIFY right after a commit
		case <-time.After(r.cfg.PollInterval): // safety net: retries, missed notifications
		case err := <-listenErr:
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("listener: %w", err)
		}
	}
}

// listen LISTENs on the notify connection and coalesces every notification
// into a single buffered wakeup. This connection does nothing else:
// WaitForNotification needs exclusive use of it.
func (r *Relay) listen(ctx context.Context, conn *pgx.Conn, wake chan<- struct{}) error {
	qctx, cancel := context.WithTimeout(ctx, r.cfg.QueryTimeout)
	_, err := conn.Exec(qctx, "LISTEN "+r.cfg.Channel)
	cancel()
	if err != nil {
		return fmt.Errorf("LISTEN %s: %w", r.cfg.Channel, err)
	}
	for {
		_, err := conn.WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("wait for notification: %w", err)
		}
		nudge(wake)
	}
}

// acquireLock blocks until this relay holds the advisory lock. A session-level
// lock is used deliberately: it lives exactly as long as the work connection,
// so a dead leader cannot strand the lock (the classic fenced-lease concern
// from DDIA 2e, ch. 9, is bounded by the TCP session lifetime).
func (r *Relay) acquireLock(ctx context.Context, work *pgx.Conn, listenErr <-chan error) error {
	for {
		qctx, cancel := context.WithTimeout(ctx, r.cfg.QueryTimeout)
		var locked bool
		err := work.QueryRow(qctx, "SELECT pg_try_advisory_lock($1)", r.cfg.AdvisoryLockKey).Scan(&locked)
		cancel()
		if err != nil {
			return fmt.Errorf("acquire advisory lock: %w", err)
		}
		if locked {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-listenErr:
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("listener while waiting for lock: %w", err)
		case <-time.After(r.cfg.LockRetry):
		}
	}
}

func (r *Relay) releaseLock(work *pgx.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var ok bool
	if err := work.QueryRow(ctx, "SELECT pg_advisory_unlock($1)", r.cfg.AdvisoryLockKey).Scan(&ok); err != nil || !ok {
		// The connection is being closed right after this anyway; the
		// server releases session advisory locks when the session ends.
		r.log.Debug("outbox relay: advisory unlock", "error", err)
	}
}

// drain claims and publishes batches until a claim comes back empty.
func (r *Relay) drain(ctx context.Context, work *pgx.Conn) error {
	for {
		events, err := r.claim(ctx, work)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			return nil
		}
		for _, evt := range events {
			if err := r.process(ctx, work, evt); err != nil {
				return err
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
	}
}

// claim atomically takes a batch of pending rows: it locks them FOR UPDATE
// SKIP LOCKED, increments their attempt counters and pre-schedules the next
// retry, then commits. Pre-scheduling means a crash anywhere after the claim
// commit results in a retry after the backoff, never a lost or
// immediately-retried storm. The commit releases the row locks before any
// network I/O happens, so no locks are held while publishing.
func (r *Relay) claim(ctx context.Context, work *pgx.Conn) ([]Event, error) {
	qctx, cancel := context.WithTimeout(ctx, r.cfg.QueryTimeout)
	defer cancel()

	tx, err := work.Begin(qctx)
	if err != nil {
		return nil, fmt.Errorf("claim: begin: %w", err)
	}
	defer tx.Rollback(context.Background()) // no-op after commit

	rows, err := tx.Query(qctx, `
		SELECT id, event_id::text, event_type, aggregate_id, payload, headers, attempts, created_at
		FROM outbox
		WHERE processed_at IS NULL
		  AND dead_at IS NULL
		  AND next_attempt_at <= now()
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, r.cfg.BatchSize)
	if err != nil {
		return nil, fmt.Errorf("claim: select: %w", err)
	}

	var events []Event
	for rows.Next() {
		var (
			evt           Event
			payload, head []byte
		)
		if err := rows.Scan(&evt.ID, &evt.EventID, &evt.EventType, &evt.AggregateID,
			&payload, &head, &evt.Attempts, &evt.CreatedAt); err != nil {
			rows.Close()
			return nil, fmt.Errorf("claim: scan: %w", err)
		}
		if len(head) > 0 && string(head) != "null" {
			if err := json.Unmarshal(head, &evt.Headers); err != nil {
				rows.Close()
				return nil, fmt.Errorf("claim: decode headers for id=%d: %w", evt.ID, err)
			}
		}
		evt.Payload = append(json.RawMessage(nil), payload...)
		evt.Attempts++ // this claim counts as an attempt
		events = append(events, evt)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claim: rows: %w", err)
	}
	if len(events) == 0 {
		return nil, nil
	}

	ids := make([]int64, len(events))
	newAttempts := make([]int32, len(events))
	delays := make([]float64, len(events))
	for i, e := range events {
		ids[i] = e.ID
		newAttempts[i] = int32(e.Attempts)
		delays[i] = r.backoff(e.Attempts).Seconds()
	}
	if _, err := tx.Exec(qctx, `
		UPDATE outbox o
		SET attempts       = u.attempts,
		    next_attempt_at = now() + make_interval(secs => u.delay_s)
		FROM unnest($1::bigint[], $2::int4[], $3::float8[]) AS u(id, attempts, delay_s)
		WHERE o.id = u.id`, ids, newAttempts, delays); err != nil {
		return nil, fmt.Errorf("claim: schedule retries: %w", err)
	}

	if err := tx.Commit(qctx); err != nil {
		return nil, fmt.Errorf("claim: commit: %w", err)
	}
	return events, nil
}

// process publishes one event and records the outcome. Every path is safe
// under a crash: the next attempt is already scheduled by claim.
func (r *Relay) process(ctx context.Context, work *pgx.Conn, evt Event) error {
	pctx, pcancel := context.WithTimeout(ctx, r.cfg.PublishTimeout)
	pubErr := r.pub.Publish(pctx, evt)
	pcancel()

	if pubErr == nil {
		r.published.Add(1)
		qctx, cancel := context.WithTimeout(ctx, r.cfg.QueryTimeout)
		defer cancel()
		if _, err := work.Exec(qctx,
			"UPDATE outbox SET processed_at = now(), last_error = NULL WHERE id = $1", evt.ID); err != nil {
			// Published but not marked: the row stays pending and will be
			// redelivered after the backoff. At-least-once, as documented.
			return fmt.Errorf("mark processed id=%d (it will be redelivered): %w", evt.ID, err)
		}
		return nil
	}

	// The relay is shutting down mid-batch; retry is already scheduled.
	if ctx.Err() != nil {
		return ctx.Err()
	}

	r.failed.Add(1)
	errStr := truncate(pubErr.Error(), 1024)

	qctx, cancel := context.WithTimeout(ctx, r.cfg.QueryTimeout)
	defer cancel()

	if evt.Attempts >= r.cfg.MaxAttempts {
		if _, err := work.Exec(qctx,
			"UPDATE outbox SET dead_at = now(), last_error = $2 WHERE id = $1 AND dead_at IS NULL",
			evt.ID, errStr); err != nil {
			return fmt.Errorf("mark dead id=%d: %w", evt.ID, err)
		}
		r.dead.Add(1)
		r.log.Error("outbox relay: event dead-lettered",
			"id", evt.ID, "event_id", evt.EventID, "type", evt.EventType,
			"aggregate", evt.AggregateID, "attempts", evt.Attempts, "error", pubErr)
		if r.cfg.OnDead != nil {
			r.cfg.OnDead(evt, pubErr)
		}
		return nil
	}

	if _, err := work.Exec(qctx,
		"UPDATE outbox SET last_error = $2 WHERE id = $1", evt.ID, errStr); err != nil {
		return fmt.Errorf("record failure id=%d: %w", evt.ID, err)
	}
	r.log.Warn("outbox relay: publish failed, will retry",
		"id", evt.ID, "event_id", evt.EventID, "type", evt.EventType,
		"attempts", evt.Attempts, "next_delay", r.backoff(evt.Attempts), "error", pubErr)
	return nil
}

// backoff returns the delay before attempt n (1-based): base * 2^(n-1),
// capped at RetryMaxDelay.
func (r *Relay) backoff(attempts int) time.Duration {
	d := r.cfg.RetryBaseDelay
	for i := 1; i < attempts && d < r.cfg.RetryMaxDelay; i++ {
		d *= 2
	}
	return min(d, r.cfg.RetryMaxDelay)
}

func nudge(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default: // a wakeup is already pending; coalesce
	}
}

func closeConn(conn *pgx.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = conn.Close(ctx)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "...(truncated)"
}
