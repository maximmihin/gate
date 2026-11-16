package outbox

import (
	"context"
	"log/slog"
)

// Publisher delivers a single event to the outside world (a Kafka producer,
// an HTTP call, a search indexer, ...). It is called sequentially, in id
// order within a batch, by exactly one relay at a time.
//
// Contract:
//   - Returning nil means "durably accepted downstream" (e.g. the broker
//     acknowledged). Anything else schedules a retry with backoff.
//   - Implementations must respect ctx cancellation and apply their own
//     timeouts; PublishTimeout additionally bounds each call.
//   - Publishing the same Event twice must be safe: delivery is
//     at-least-once, and consumers deduplicate by Event.EventID.
type Publisher interface {
	Publish(ctx context.Context, evt Event) error
}

// PublisherFunc adapts a function to the Publisher interface.
type PublisherFunc func(ctx context.Context, evt Event) error

func (f PublisherFunc) Publish(ctx context.Context, evt Event) error { return f(ctx, evt) }

// LogPublisher logs every event and succeeds. Handy for demos, smoke tests
// and as a no-op sink.
type LogPublisher struct {
	Logger *slog.Logger
}

// Publish logs the event.
func (p LogPublisher) Publish(_ context.Context, evt Event) error {
	logger := p.Logger
	if logger == nil {
		logger = slog.Default()
	}
	logger.Info("outbox event published",
		"id", evt.ID, "event_id", evt.EventID, "type", evt.EventType,
		"aggregate", evt.AggregateID, "payload", string(evt.Payload))
	return nil
}
