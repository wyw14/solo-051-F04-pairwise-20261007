package service

import (
	"testing"
	"time"

	"github.com/wyw14/cry-161/internal/model"
	"github.com/wyw14/cry-161/internal/optical"
)

func runtimeSample(runID model.ID, nm float64) model.ThicknessSample {
	return model.ThicknessSample{RunID: runID, Frequency: 10, Transmission: 0.9, Nanometers: nm, Valid: true, At: time.Now().UTC()}
}

func TestCaptureAssignsMonotonicSequence(t *testing.T) {
	runtime := NewRuntime()
	runID := model.NewID()
	e1, err := runtime.Capture(runtimeSample(runID, 1), 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Capture(runtimeSample(runID, 2), 1); err != nil {
		t.Fatal(err)
	}
	trace := runtime.Traces.Trace(runID)
	if len(trace.Thickness) != 2 || trace.Thickness[0].Seq != 1 || trace.Thickness[1].Seq != 2 {
		t.Fatalf("trace seqs = %+v, want 1,2", trace.Thickness)
	}
	if runtime.Broadcast.LastSequence() != 2 {
		t.Fatalf("broadcast high watermark = %d, want 2", runtime.Broadcast.LastSequence())
	}
	_ = e1
}

func TestRuntimeConsumerReplaysGapFromTrace(t *testing.T) {
	runtime := NewRuntime()
	runID := model.NewID()

	// Attach before capturing, with a buffer of one, then flood: only one
	// sample stays live, the rest drop and must come from the trace.
	consumerMonitor := optical.NewMonitor()
	consumer := runtime.NewConsumer(consumerMonitor, optical.ConsumerOptions{
		Buffer:         1,
		Events:         64,
		ReplayInterval: 5 * time.Millisecond,
		MaxReplay:      128,
	})
	for nm := float64(1); nm <= 5; nm++ {
		if _, err := runtime.Capture(runtimeSample(runID, nm), 1); err != nil {
			t.Fatal(err)
		}
	}
	stop := make(chan struct{})
	go consumer.Run(stop)
	defer func() { close(stop); consumer.Close() }()

	seen := map[int64]bool{}
	deadline := time.After(2 * time.Second)
	for len(seen) < 5 {
		select {
		case e := <-consumer.Events():
			if e.Kind == optical.EventLive || e.Kind == optical.EventReplay {
				if seen[e.Sample.Seq] {
					t.Fatalf("seq %d delivered twice via %s", e.Sample.Seq, e.Kind)
				}
				seen[e.Sample.Seq] = true
			}
		case <-deadline:
			t.Fatalf("missing frames, got %v", seen)
		}
	}
	for seq := int64(1); seq <= 5; seq++ {
		if !seen[seq] {
			t.Fatalf("seq %d never delivered", seq)
		}
	}

	// The consumer-local safety monitor may see the one live sample only;
	// replayed trace samples must never re-enter it.
	for _, s := range consumerMonitor.Samples() {
		if s.Seq > 1 {
			t.Fatalf("replayed seq %d entered consumer monitor", s.Seq)
		}
	}
}

func TestRuntimeReplayReport(t *testing.T) {
	runtime := NewRuntime()
	runID := model.NewID()
	for nm := float64(1); nm <= 3; nm++ {
		if _, err := runtime.Capture(runtimeSample(runID, nm), 1); err != nil {
			t.Fatal(err)
		}
	}
	report, err := runtime.Replay(t.Context(), optical.Cursor{Seq: 1}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Samples) != 2 || report.Samples[0].Seq != 2 || report.Cursor.Seq != 3 {
		t.Fatalf("replay report = %+v", report)
	}
}
