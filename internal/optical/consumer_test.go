package optical

import (
	"context"
	"github.com/wyw14/cry-161/internal/journal"
	"github.com/wyw14/cry-161/internal/model"
	"testing"
	"time"
)

func drainGap(t *testing.T, c *Consumer, want int, timeout time.Duration) []model.ThicknessSample {
	t.Helper()
	out := make([]model.ThicknessSample, 0, want)
	deadline := time.After(timeout)
	for len(out) < want {
		select {
		case s := <-c.Gap():
			out = append(out, s)
		case <-deadline:
			t.Fatalf("drained %d gap samples, want %d", len(out), want)
		}
	}
	return out
}

func TestReplayNeverReachesMonitor(t *testing.T) {
	store := journal.NewTraceStore()
	for seq := int64(1); seq <= 3; seq++ {
		store.AppendThickness(model.ThicknessSample{RunID: "run", Nanometers: float64(seq)})
	}
	monitor := NewMonitor()
	c := NewConsumer(monitor, make(chan model.ThicknessSample, 8))
	stop := make(chan struct{})
	defer close(stop)
	go c.Run(stop)

	if n := c.ReplayGap(store); n != 3 {
		t.Fatalf("enqueued %d, want 3", n)
	}
	got := drainGap(t, c, 3, time.Second)
	if got[0].Seq != 1 || got[2].Seq != 3 {
		t.Fatalf("gap order wrong: %d %d", got[0].Seq, got[2].Seq)
	}
	if c.Frontier() != 3 {
		t.Fatalf("frontier = %d, want 3", c.Frontier())
	}
	if !monitor.Latest().At.IsZero() {
		t.Fatal("replayed sample entered the safety monitor")
	}
}

// A dropped hole must not be hidden by later live samples: the cursor is the
// contiguous frontier, not the high-water mark.
func TestCursorIsContiguousFrontierAndReplayFillsHole(t *testing.T) {
	store := journal.NewTraceStore()
	for seq := int64(1); seq <= 3; seq++ {
		store.AppendThickness(model.ThicknessSample{RunID: "run", Nanometers: float64(seq)})
	}
	monitor := NewMonitor()
	live := make(chan model.ThicknessSample, 8)
	c := NewConsumer(monitor, live)
	stop := make(chan struct{})
	defer close(stop)
	go c.Run(stop)

	// seq 1 is lost on the wire; 2 and 3 arrive live.
	live <- sampleWith(2)
	live <- sampleWith(3)
	if !waitFor(func() bool { return monitor.Latest().Seq == 3 }, 50*time.Millisecond) {
		t.Fatal("live samples did not reach monitor")
	}
	if c.Frontier() != 0 {
		t.Fatalf("frontier = %d, want 0 while seq 1 is missing", c.Frontier())
	}

	_, cursor := c.Reconnect()
	if cursor.LastSeq != 0 {
		t.Fatalf("reconnect cursor = %d, want 0 (hole at seq 1)", cursor.LastSeq)
	}
	if n := c.ReplayGap(store); n != 3 {
		t.Fatalf("trace replay should read all 3 from cursor 0, got %d", n)
	}
	// Only seq 1 reaches the gap reader; 2 and 3 were covered live.
	got := drainGap(t, c, 1, time.Second)
	if got[0].Seq != 1 {
		t.Fatalf("gap emitted seq %d, want only the missing seq 1", got[0].Seq)
	}
	if c.GapSkipped() != 2 || !waitFor(func() bool { return c.GapSkipped() == 2 }, time.Second) {
		t.Fatalf("skipped %d overlaps, want 2", c.GapSkipped())
	}
	if c.Frontier() != 3 {
		t.Fatalf("frontier = %d, want 3 after gap fill", c.Frontier())
	}
	if monitor.Latest().Seq != 3 || len(monitor.Samples()) != 2 {
		t.Fatal("replay leaked into the monitor")
	}
}

func TestRepeatedReplayDoesNotDoubleEmit(t *testing.T) {
	store := journal.NewTraceStore()
	store.AppendThickness(model.ThicknessSample{RunID: "run", Nanometers: 1})
	monitor := NewMonitor()
	c := NewConsumer(monitor, make(chan model.ThicknessSample, 8))
	stop := make(chan struct{})
	defer close(stop)
	go c.Run(stop)

	c.ReplayGap(store)
	drainGap(t, c, 1, time.Second)
	// A raw tail sample arriving twice (e.g. a racing second replay) must not
	// be emitted a second time even though the cursor has moved past it.
	c.ReplayIn() <- sampleWith(1)
	select {
	case s := <-c.Gap():
		t.Fatalf("duplicate replay emitted seq %d", s.Seq)
	case <-time.After(30 * time.Millisecond):
	}
	if c.GapSkipped() != 1 {
		t.Fatalf("skipped = %d, want 1", c.GapSkipped())
	}
}

// A stalled replay consumer must not delay the realtime path that feeds the
// safety controls.
func TestReplayBackpressureDoesNotBlockLive(t *testing.T) {
	monitor := NewMonitor()
	live := make(chan model.ThicknessSample, 8)
	c := NewConsumer(monitor, live)
	stop := make(chan struct{})
	go c.Run(stop)

	pumpDone := make(chan struct{})
	go func() {
		for seq := int64(1); seq <= 400; seq++ {
			c.ReplayIn() <- sampleWith(seq)
		}
		close(pumpDone)
	}()
	// Drain the gap side so the pump can finish, while an interleaved live
	// sample keeps reaching the monitor immediately.
	drainDone := make(chan struct{})
	go func() {
		for range c.Gap() {
			if c.Frontier() >= 400 {
				break
			}
		}
		close(drainDone)
	}()
	live <- model.ThicknessSample{RunID: "run", Seq: 500, Nanometers: 500, Valid: true}
	if !waitFor(func() bool { return monitor.Latest().Seq == 500 }, 100*time.Millisecond) {
		t.Fatal("live delivery stalled behind the replay pump")
	}
	<-pumpDone
	<-drainDone
	close(stop)
}

func TestReconnectPolicyCarriesCursor(t *testing.T) {
	c := NewConsumer(NewMonitor(), make(chan model.ThicknessSample, 8))
	stop := make(chan struct{})
	defer close(stop)
	go c.Run(stop)
	policy := DefaultReconnectPolicy()
	result, err := policy.Connect(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if result.Channel == nil || result.Cursor.LastSeq != 0 {
		t.Fatalf("bad first connect: %+v", result.Cursor)
	}
	result.Channel <- sampleWith(1)
	if !waitFor(func() bool { return c.Frontier() == 1 }, 50*time.Millisecond) {
		t.Fatal("live sample not handled")
	}
	again, err := policy.Connect(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if again.Cursor.LastSeq != 1 || again.Cursor.Reconnects != 2 {
		t.Fatalf("cursor after reconnect = %+v, want seq=1 reconnects=2", again.Cursor)
	}
}

func waitFor(cond func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return cond()
}
