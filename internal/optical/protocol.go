package optical

import (
	"context"
	"errors"
	"github.com/wyw14/cry-161/internal/model"
)

// Cursor is an opaque position in the thickness sample stream. Seq zero
// means "before the first sample"; a consumer resumes strictly after the
// sequence it names.
type Cursor struct {
	Seq int64 `json:"seq"`
}

// SubscriptionStatus describes a live subscription. Dropped counts samples
// the broadcaster could not enqueue because the channel was full; LastSeq is
// the highest sequence that reached the subscription; Cursor is the position
// the consumer reconnects with to fill any gap; Overflow is set when at least
// one sample was dropped.
type SubscriptionStatus struct {
	Dropped  int64  `json:"dropped"`
	LastSeq  int64  `json:"last_seq"`
	Cursor   Cursor `json:"cursor"`
	Overflow bool   `json:"overflow"`
}

// EventKind tags frames emitted by a Consumer.
type EventKind string

const (
	// EventLive is a sample that arrived over the real-time channel.
	EventLive EventKind = "live"
	// EventReplay is a sample recovered from the trace to fill a gap.
	// Replay events never reach the safety Monitor.
	EventReplay EventKind = "replay"
	// EventStatus reports subscription counters (drops, cursor).
	EventStatus EventKind = "status"
)

// Event is a presentation-layer frame: live samples, recovered gap samples,
// and subscription status updates. Emission is best-effort and never blocks
// the safety path.
type Event struct {
	Kind   EventKind             `json:"kind"`
	Sample model.ThicknessSample `json:"sample,omitempty"`
	Status SubscriptionStatus    `json:"status,omitempty"`
}

// ReplayReader replays persisted samples whose sequence is strictly greater
// than cursor.Seq. Implementations must be safe for concurrent use and must
// never be on the safety-control hot path.
type ReplayReader interface {
	ReplayAfter(ctx context.Context, cursor Cursor, limit int) ([]model.ThicknessSample, error)
}

// ReplayReaderFunc adapts a plain function to ReplayReader.
type ReplayReaderFunc func(ctx context.Context, cursor Cursor, limit int) ([]model.ThicknessSample, error)

func (f ReplayReaderFunc) ReplayAfter(ctx context.Context, cursor Cursor, limit int) ([]model.ThicknessSample, error) {
	return f(ctx, cursor, limit)
}

// ErrReplayUnavailable is returned when a consumer has no trace reader.
var ErrReplayUnavailable = errors.New("optical: no replay reader configured")

// ErrGapExpired is returned when the cursor points before the oldest sample
// the trace still retains; the missing gap can no longer be reconstructed.
var ErrGapExpired = errors.New("optical: replay gap expired")

// ErrConsumerStopped is returned by reconnect calls after Close.
var ErrConsumerStopped = errors.New("optical: consumer stopped")

// ReplayReport is the outcome of a gap replay.
type ReplayReport struct {
	From    Cursor                  `json:"from"`
	Samples []model.ThicknessSample `json:"samples"`
	Cursor  Cursor                  `json:"cursor"`
}
