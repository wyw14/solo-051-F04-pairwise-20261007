package journal

import (
	"context"
	"errors"
	"testing"

	"github.com/wyw14/cry-161/internal/model"
)

func traceSample(seq int64) model.ThicknessSample {
	return model.ThicknessSample{RunID: "run", Seq: seq, Nanometers: float64(seq), Valid: true}
}

func TestTraceStoreReplayAfter(t *testing.T) {
	store := NewTraceStoreWithDepth(4)
	for seq := int64(1); seq <= 4; seq++ {
		store.AppendThickness(traceSample(seq))
	}
	got, err := store.ReplayAfter(context.Background(), 1, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].Seq != 2 || got[2].Seq != 4 {
		t.Fatalf("replay = %+v, want seqs 2,3,4", got)
	}

	limited, err := store.ReplayAfter(context.Background(), 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(limited) != 2 || limited[0].Seq != 1 {
		t.Fatalf("limited replay = %+v, want 1,2", limited)
	}
}

func TestTraceStoreReplayWrapsRing(t *testing.T) {
	store := NewTraceStoreWithDepth(3)
	for seq := int64(1); seq <= 5; seq++ {
		store.AppendThickness(traceSample(seq))
	}
	// Only 3,4,5 retained, in ascending order despite ring wraparound.
	got, err := store.ReplayAfter(context.Background(), 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].Seq != 3 || got[2].Seq != 5 {
		t.Fatalf("wrapped replay = %+v, want seqs 3,4,5", got)
	}
	if oldest := store.OldestSequence(); oldest != 3 {
		t.Fatalf("oldest = %d, want 3", oldest)
	}
}

func TestTraceStoreExpiredCursor(t *testing.T) {
	store := NewTraceStoreWithDepth(2)
	for seq := int64(1); seq <= 4; seq++ {
		store.AppendThickness(traceSample(seq))
	}
	// Retained oldest is seq 3; a cursor at seq 1 means seq 2 is gone.
	_, err := store.ReplayAfter(context.Background(), 1, 100)
	if !errors.Is(err, ErrCursorExpired) {
		t.Fatalf("err = %v, want ErrCursorExpired", err)
	}
	// Cursor exactly one before the oldest is still satisfiable.
	got, err := store.ReplayAfter(context.Background(), 2, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Seq != 3 {
		t.Fatalf("replay from oldest-1 = %+v, want 3,4", got)
	}
}

func TestTraceStorePerRunTraceUnaffected(t *testing.T) {
	store := NewTraceStoreWithDepth(2)
	s1 := model.ThicknessSample{RunID: "a", Seq: 1, Valid: true}
	s2 := model.ThicknessSample{RunID: "b", Seq: 2, Valid: true}
	s3 := model.ThicknessSample{RunID: "a", Seq: 3, Valid: true}
	store.AppendThickness(s1)
	store.AppendThickness(s2)
	store.AppendThickness(s3) // evicts s1 from replay ring
	trace := store.Trace("a")
	if len(trace.Thickness) != 2 || trace.Thickness[0].Seq != 1 || trace.Thickness[1].Seq != 3 {
		t.Fatalf("per-run trace = %+v, want seqs 1,3", trace.Thickness)
	}
}
