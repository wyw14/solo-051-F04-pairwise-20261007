package optical

import (
	"context"
	"fmt"
	"time"
)

// ReconnectPolicy bounds transport rotation attempts made by a consumer.
type ReconnectPolicy struct {
	Attempts int
	Backoff  time.Duration
}

func DefaultReconnectPolicy() ReconnectPolicy {
	return ReconnectPolicy{Attempts: 3, Backoff: time.Millisecond}
}

// Connect rotates the consumer's live subscription and returns the cursor
// from which the consumer is filling its replay gap. The live channel itself
// is owned by the consumer; callers resume reading from Consumer.Events.
func (p ReconnectPolicy) Connect(ctx context.Context, consumer *Consumer) (Cursor, error) {
	if consumer == nil {
		return Cursor{}, fmt.Errorf("consumer is required")
	}
	if p.Attempts < 1 || p.Backoff < 0 {
		return Cursor{}, fmt.Errorf("reconnect policy is invalid")
	}
	var lastErr error
	for attempt := 0; attempt < p.Attempts; attempt++ {
		cursor, err := consumer.Resubscribe(ctx)
		if err == nil {
			return cursor, nil
		}
		lastErr = err
		if attempt == p.Attempts-1 {
			break
		}
		timer := time.NewTimer(p.Backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return Cursor{}, ctx.Err()
		case <-timer.C:
		}
	}
	return Cursor{}, fmt.Errorf("optical reconnect exhausted: %w", lastErr)
}

// ReplayGap fetches samples strictly after cursor directly from a trace
// reader. It is the out-of-band path used when a caller wants to repair a
// gap without attaching a live consumer. Replayed samples bypass the safety
// Monitor by construction: this function only returns data, it feeds nothing.
func ReplayGap(ctx context.Context, reader ReplayReader, cursor Cursor, limit int) (ReplayReport, error) {
	if reader == nil {
		return ReplayReport{From: cursor, Cursor: cursor}, ErrReplayUnavailable
	}
	if limit < 1 {
		limit = 256
	}
	samples, err := reader.ReplayAfter(ctx, cursor, limit)
	if err != nil {
		return ReplayReport{From: cursor, Cursor: cursor}, err
	}
	high := cursor.Seq
	for _, sample := range samples {
		if sample.Seq > high {
			high = sample.Seq
		}
	}
	return ReplayReport{
		From:    cursor,
		Samples: samples,
		Cursor:  Cursor{Seq: high},
	}, nil
}
