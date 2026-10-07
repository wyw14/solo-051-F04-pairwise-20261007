package optical

import (
	"context"
	"fmt"
	"github.com/wyw14/cry-161/internal/model"
	"time"
)

type ReconnectPolicy struct {
	Attempts int
	Backoff  time.Duration
}

func DefaultReconnectPolicy() ReconnectPolicy {
	return ReconnectPolicy{Attempts: 3, Backoff: time.Millisecond}
}

// ReconnectResult carries the fresh live channel and the cursor that locates
// the gap in the trace: call ReplayAfter(cursor.LastSeq) (or
// Consumer.ReplayGap) to recover everything missed while disconnected.
type ReconnectResult struct {
	Channel chan model.ThicknessSample
	Cursor  Cursor
}

func (p ReconnectPolicy) Connect(ctx context.Context, consumer *Consumer) (ReconnectResult, error) {
	if consumer == nil {
		return ReconnectResult{}, fmt.Errorf("consumer is required")
	}
	if p.Attempts < 1 || p.Backoff < 0 {
		return ReconnectResult{}, fmt.Errorf("reconnect policy is invalid")
	}
	for attempt := 0; attempt < p.Attempts; attempt++ {
		select {
		case <-ctx.Done():
			return ReconnectResult{}, ctx.Err()
		default:
		}
		channel, cursor := consumer.Reconnect()
		if channel != nil {
			return ReconnectResult{Channel: channel, Cursor: cursor}, nil
		}
		timer := time.NewTimer(p.Backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ReconnectResult{}, ctx.Err()
		case <-timer.C:
		}
	}
	return ReconnectResult{}, fmt.Errorf("optical reconnect exhausted")
}
