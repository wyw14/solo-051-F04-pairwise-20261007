package journal

import (
	"github.com/wyw14/cry-161/internal/model"
	"testing"
)

func TestAppendAssignsMonotonicSequence(t *testing.T) {
	store := NewTraceStore()
	run := model.ID("run-1")
	first := store.AppendThickness(model.ThicknessSample{RunID: run, Nanometers: 1})
	second := store.AppendThickness(model.ThicknessSample{RunID: run, Nanometers: 2})
	if first != 1 || second != 2 {
		t.Fatalf("sequences = %d,%d, want 1,2", first, second)
	}
	if store.LastSequence() != 2 {
		t.Fatalf("last sequence = %d, want 2", store.LastSequence())
	}
	trace := store.Trace(run)
	if len(trace.Thickness) != 2 || trace.Thickness[0].Seq != 1 || trace.Thickness[1].Seq != 2 {
		t.Fatalf("trace lost sequence metadata: %+v", trace.Thickness)
	}
}

func TestReplayRespectsCursorPerRun(t *testing.T) {
	store := NewTraceStore()
	runA := model.ID("run-a")
	for i := 0; i < 5; i++ {
		store.AppendThickness(model.ThicknessSample{RunID: runA, Nanometers: float64(i)})
	}
	// Another run interleaves the global sequence space.
	store.AppendThickness(model.ThicknessSample{RunID: model.ID("run-b"), Nanometers: 9})

	gap := store.Replay(runA, 2)
	if len(gap) != 3 {
		t.Fatalf("replay length = %d, want 3", len(gap))
	}
	for i, sample := range gap {
		if sample.Seq != int64(3+i) {
			t.Fatalf("replay[%d] seq = %d, want %d", i, sample.Seq, 3+i)
		}
	}
	if store.Replay(runA, 5) != nil && len(store.Replay(runA, 5)) != 0 {
		t.Fatal("cursor at the tip should replay nothing")
	}
}

func TestReplayAfterAcrossRunsIsOrdered(t *testing.T) {
	store := NewTraceStore()
	store.AppendThickness(model.ThicknessSample{RunID: "a"}) // seq 1
	store.AppendThickness(model.ThicknessSample{RunID: "b"}) // seq 2
	store.AppendThickness(model.ThicknessSample{RunID: "a"}) // seq 3

	gap := store.ReplayAfter(1)
	if len(gap) != 2 || gap[0].Seq != 2 || gap[1].Seq != 3 {
		t.Fatalf("global replay wrong order/content: %+v", gap)
	}
}
