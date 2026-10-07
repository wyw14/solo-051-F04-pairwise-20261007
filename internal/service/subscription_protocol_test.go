package service

import (
	"context"
	"github.com/wyw14/cry-161/internal/model"
	"github.com/wyw14/cry-161/internal/optical"
	"testing"
	"time"
)

// End-to-end at the runtime boundary: Capture numbers each sample in the trace
// before the non-blocking broadcast, a saturated subscriber reports drops, and
// the same trace satisfies the reconnect cursor without touching the monitor.
func TestCaptureSubscriptionProtocol(t *testing.T) {
	runtime := NewRuntime()
	id, ch, _ := runtime.Broadcast.Subscribe(1) // size-1: floods immediately

	runID := model.ID("run-proto")
	const total = 6
	for i := 1; i <= total; i++ {
		sample := model.ThicknessSample{RunID: runID, Frequency: 10, Transmission: 0.9, Nanometers: float64(i), Valid: true}
		if _, err := runtime.Capture(sample, 1); err != nil {
			t.Fatalf("capture %d: %v", i, err)
		}
	}
	// Every captured sample carries the trace-assigned global sequence.
	trace := runtime.Traces.Trace(runID)
	if len(trace.Thickness) != total {
		t.Fatalf("trace length = %d, want %d", len(trace.Thickness), total)
	}
	for i, sample := range trace.Thickness {
		if sample.Seq != int64(i+1) {
			t.Fatalf("trace[%d] seq = %d, want %d", i, sample.Seq, i+1)
		}
	}

	status, ok := runtime.Broadcast.Status(id)
	if !ok {
		t.Fatal("subscription missing")
	}
	if status.LastSeq != total {
		t.Fatalf("last seq = %d, want %d", status.LastSeq, total)
	}
	if status.Dropped == 0 {
		t.Fatal("saturated subscriber reported no drops")
	}
	if status.Dropped != total-status.LastDelivered {
		t.Fatalf("delivered=%d dropped=%d does not partition %d publishes", status.LastDelivered, status.Dropped, total)
	}
	// Drain the one buffered live sample so the channel does not leak.
drainLoop:
	for {
		select {
		case <-ch:
		default:
			break drainLoop
		}
	}
	runtime.Broadcast.Unsubscribe(id)

	// Replay the subscriber's gap from its delivery cursor.
	gap := runtime.Traces.Replay(runID, status.LastDelivered)
	if int64(len(gap)) != status.Dropped {
		t.Fatalf("replayed %d, want %d dropped", len(gap), status.Dropped)
	}
	for i, sample := range gap {
		if want := status.LastDelivered + int64(i) + 1; sample.Seq != want {
			t.Fatalf("gap[%d] seq = %d, want %d", i, sample.Seq, want)
		}
	}

	// Consumer-side replay stays out of the safety monitor even though the
	// monitor already holds every live Capture.
	monitor := optical.NewMonitor()
	consumer := optical.NewConsumer(monitor, make(chan model.ThicknessSample, 8))
	stop := make(chan struct{})
	go consumer.Run(stop)
	enqueued := consumer.ReplayGap(runtime.Traces)
	received := 0
	deadline := time.After(time.Second)
	for received < enqueued {
		select {
		case <-consumer.Gap():
			received++
		case <-deadline:
			t.Fatalf("gap stalled at %d/%d", received, enqueued)
		}
	}
	close(stop)
	if !monitor.Latest().At.IsZero() {
		t.Fatal("replayed samples leaked into a fresh monitor")
	}
	if consumer.Frontier() != int64(total) {
		t.Fatalf("frontier = %d, want %d", consumer.Frontier(), total)
	}
}

func TestAuditCoversSubscriptionProtocol(t *testing.T) {
	t.Setenv("OPTICCOAT_DATA", t.TempDir())
	svc := New()
	runtime := NewRuntime()
	report := svc.Audit(context.Background(), runtime)
	for _, check := range []string{"optical_subscription", "optical_gap_replay", "optical_reconnect"} {
		value, ok := report.Checks[check]
		if !ok {
			t.Fatalf("audit missing check %s", check)
		}
		if !report.Passed {
			t.Fatalf("audit failed at %s: %s", check, value)
		}
	}
}
