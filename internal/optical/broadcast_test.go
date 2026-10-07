package optical

import (
	"github.com/wyw14/cry-161/internal/model"
	"testing"
	"time"
)

func sampleWith(seq int64) model.ThicknessSample {
	return model.ThicknessSample{RunID: "run", Seq: seq, Nanometers: float64(seq), Valid: true}
}

func TestBroadcasterDeliversAndTracksSequence(t *testing.T) {
	b := NewBroadcaster()
	id, ch, start := b.Subscribe(4)
	if start.LastSeq != 0 || start.Dropped != 0 {
		t.Fatalf("fresh subscription should start at zero stats, got %+v", start)
	}
	b.Publish(sampleWith(1))
	b.Publish(sampleWith(2))
	got := []int64{(<-(ch)).Seq, (<-(ch)).Seq}
	if got[0] != 1 || got[1] != 2 {
		t.Fatalf("out of order delivery: %v", got)
	}
	status, ok := b.Status(id)
	if !ok {
		t.Fatal("subscription disappeared")
	}
	if status.LastSeq != 2 || status.LastDelivered != 2 || status.Dropped != 0 {
		t.Fatalf("unexpected status %+v", status)
	}
	b.Unsubscribe(id)
	if _, ok := b.Status(id); ok {
		t.Fatal("status reported after unsubscribe")
	}
}

func TestBroadcasterCountsDropsWithoutBlocking(t *testing.T) {
	b := NewBroadcaster()
	id, ch, _ := b.Subscribe(1)
	// First sample fills the single slot; every later sample must be dropped
	// for this subscriber rather than stall Publish.
	begin := time.Now()
	const total = 10000
	for seq := int64(1); seq <= total; seq++ {
		b.Publish(sampleWith(seq))
	}
	if elapsed := time.Since(begin); elapsed > 100*time.Millisecond {
		t.Fatalf("publishing to a saturated subscriber blocked for %s", elapsed)
	}
	status, _ := b.Status(id)
	if status.LastSeq != total {
		t.Fatalf("last seq = %d, want %d", status.LastSeq, total)
	}
	if status.Dropped != total-1 {
		t.Fatalf("dropped = %d, want %d", status.Dropped, total-1)
	}
	if status.LastDelivered != 1 {
		t.Fatalf("last delivered = %d, want 1", status.LastDelivered)
	}
	if (<-ch).Seq != 1 {
		t.Fatal("buffered sample was not the first published")
	}
}

func TestBroadcasterPerSubscriberCounters(t *testing.T) {
	b := NewBroadcaster()
	slowID, slowCh, _ := b.Subscribe(1)
	fastID, fastCh, _ := b.Subscribe(8)
	for seq := int64(1); seq <= 4; seq++ {
		b.Publish(sampleWith(seq))
	}
	slow, _ := b.Status(slowID)
	fast, _ := b.Status(fastID)
	if slow.Dropped != 3 || slow.LastSeq != 4 {
		t.Fatalf("slow subscriber stats wrong: %+v", slow)
	}
	if fast.Dropped != 0 || fast.LastDelivered != 4 {
		t.Fatalf("fast subscriber stats wrong: %+v", fast)
	}
	if len(fastCh) != 4 || len(slowCh) != 1 {
		t.Fatalf("buffer lengths slow=%d fast=%d", len(slowCh), len(fastCh))
	}
}
