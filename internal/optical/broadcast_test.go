package optical

import (
	"testing"
	"time"

	"github.com/wyw14/cry-161/internal/model"
)

func sample(seq int64) model.ThicknessSample {
	return model.ThicknessSample{RunID: "run-1", Seq: seq, Frequency: float64(seq), Nanometers: float64(seq), Valid: true, At: time.Unix(1000+seq, 0).UTC()}
}

func TestBroadcasterDropsCountedAndNeverBlocks(t *testing.T) {
	b := NewBroadcaster()
	sub := b.Subscribe(1)

	// Fill the single-slot buffer; the next publishes must drop immediately
	// rather than block.
	b.Publish(sample(1))
	done := make(chan struct{})
	go func() {
		b.Publish(sample(2))
		b.Publish(sample(3))
		b.Publish(sample(4))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Publish blocked on a full subscriber buffer")
	}

	status := sub.Status()
	if status.Dropped != 3 {
		t.Fatalf("Dropped = %d, want 3", status.Dropped)
	}
	if status.LastSeq != 1 {
		t.Fatalf("LastSeq = %d, want 1", status.LastSeq)
	}
	if !status.Overflow {
		t.Fatal("Overflow = false, want true")
	}
	if b.LastSequence() != 4 {
		t.Fatalf("LastSequence = %d, want 4", b.LastSequence())
	}

	got := <-sub.Channel()
	if got.Seq != 1 {
		t.Fatalf("buffered sample seq = %d, want 1", got.Seq)
	}
	sub.Unsubscribe()
	if b.SubscriberCount() != 0 {
		t.Fatal("subscription still registered after unsubscribe")
	}
}

func TestBroadcasterIndependentSubscribers(t *testing.T) {
	b := NewBroadcaster()
	s1 := b.Subscribe(8)
	s2 := b.Subscribe(1)
	b.Publish(sample(1))
	// s2 drops seq 2 and 3; s1 must not be affected.
	b.Publish(sample(2))
	b.Publish(sample(3))

	if st := s2.Status(); st.Dropped != 2 {
		t.Fatalf("s2 drops = %d, want 2", st.Dropped)
	}
	if st := s1.Status(); st.Dropped != 0 || st.LastSeq != 3 {
		t.Fatalf("s1 status = %+v, want no drops and last 3", st)
	}
	s1.Unsubscribe()
	s2.Unsubscribe()
}
