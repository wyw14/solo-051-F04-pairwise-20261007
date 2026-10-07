package optical

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/wyw14/cry-161/internal/model"
)

// memReader is an in-memory ReplayReader backed by a seq-indexed map.
type memReader struct {
	mu      sync.Mutex
	samples map[int64]model.ThicknessSample
	expired int64
	fail    bool
}

func newMemReader() *memReader {
	return &memReader{samples: map[int64]model.ThicknessSample{}}
}

func (r *memReader) add(s model.ThicknessSample) {
	r.mu.Lock()
	r.samples[s.Seq] = s
	r.mu.Unlock()
}

func (r *memReader) ReplayAfter(ctx context.Context, cursor Cursor, limit int) ([]model.ThicknessSample, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail {
		return nil, context.DeadlineExceeded
	}
	if cursor.Seq > 0 && cursor.Seq < r.expired {
		return nil, ErrGapExpired
	}
	out := make([]model.ThicknessSample, 0, limit)
	for seq := cursor.Seq + 1; len(out) < limit; seq++ {
		s, ok := r.samples[seq]
		if !ok {
			break
		}
		out = append(out, s)
	}
	return out, nil
}

func drainEvents(t *testing.T, events <-chan Event, want int, timeout time.Duration) []Event {
	t.Helper()
	got := make([]Event, 0, want)
	deadline := time.After(timeout)
	for len(got) < want {
		select {
		case event := <-events:
			got = append(got, event)
		case <-deadline:
			t.Fatalf("timed out waiting for %d events, got %d: %+v", want, len(got), got)
		}
	}
	return got
}

func waitFor(t *testing.T, check func() bool, timeout time.Duration, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// collectSampleFrames drains until want distinct sample frames (live or
// replay, status frames ignored) arrive or the timeout fires.
func collectSampleFrames(t *testing.T, c *Consumer, want int, timeout time.Duration) map[int64]EventKind {
	t.Helper()
	frames := map[int64]EventKind{}
	deadline := time.After(timeout)
	for len(frames) < want {
		select {
		case e := <-c.Events():
			if e.Kind == EventLive || e.Kind == EventReplay {
				if kind, ok := frames[e.Sample.Seq]; ok {
					t.Fatalf("seq %d delivered twice: %s then %s", e.Sample.Seq, kind, e.Kind)
				}
				frames[e.Sample.Seq] = e.Kind
			}
		case <-deadline:
			t.Fatalf("wanted %d distinct sample frames, got %d: %v", want, len(frames), frames)
		}
	}
	return frames
}

func TestConsumerLiveReachesMonitorAndEvents(t *testing.T) {
	b := NewBroadcaster()
	m := NewMonitor()
	c := NewConsumer(b, m, ConsumerOptions{Buffer: 4, ReplayInterval: 5 * time.Millisecond})
	stop := make(chan struct{})
	go c.Run(stop)
	defer func() { close(stop); c.Close() }()

	b.Publish(sample(1))
	if e := drainEvents(t, c.Events(), 1, time.Second)[0]; e.Kind != EventLive || e.Sample.Seq != 1 {
		t.Fatalf("event = %+v, want live seq 1", e)
	}

	waitFor(t, func() bool { return m.Latest().Seq == 1 }, time.Second, "monitor to see seq 1")
	if got := c.Cursor().Seq; got != 1 {
		t.Fatalf("cursor = %d, want 1", got)
	}
}

func TestConsumerDroppedSamplesReplayedAndDoNotEnterMonitor(t *testing.T) {
	b := NewBroadcaster()
	m := NewMonitor()
	reader := newMemReader()
	// Trace holds only the gap range; seq 1 is delivered live.
	for seq := int64(2); seq <= 5; seq++ {
		reader.add(sample(seq))
	}
	// Subscribe before Run: the pump is not draining yet, so publishes to a
	// buffer-1 channel deterministically keep seq 1 and drop 2..5.
	c := NewConsumer(b, m, ConsumerOptions{
		Buffer:         1,
		Events:         64,
		Replay:         reader,
		ReplayInterval: 5 * time.Millisecond,
		MaxReplay:      128,
	})
	for seq := int64(1); seq <= 5; seq++ {
		b.Publish(sample(seq))
	}
	stop := make(chan struct{})
	go c.Run(stop)
	defer func() { close(stop); c.Close() }()

	frames := collectSampleFrames(t, c, 5, 2*time.Second)
	for seq := int64(1); seq <= 5; seq++ {
		if _, ok := frames[seq]; !ok {
			t.Fatalf("seq %d missing from event stream", seq)
		}
	}
	waitFor(t, func() bool { return c.Cursor().Seq == 5 }, time.Second, "cursor to reach 5")

	if dropped := c.Status().Dropped; dropped != 4 {
		t.Fatalf("cumulative dropped = %d, want 4", dropped)
	}

	// Safety invariant: the gap samples 2..5 are replay data and must never
	// reach the monitor, regardless of which path wins seq 1.
	waitFor(t, func() bool {
		for _, s := range m.Samples() {
			if s.Seq > 1 {
				return false
			}
		}
		return len(m.Samples()) <= 1
	}, time.Second, "monitor to stay clear of replay samples")
	for _, s := range m.Samples() {
		if s.Seq > 1 {
			t.Fatalf("replayed seq %d entered the safety monitor", s.Seq)
		}
	}
}

func TestConsumerNoDuplicateWhenLiveAndReplayRace(t *testing.T) {
	b := NewBroadcaster()
	m := NewMonitor()
	reader := newMemReader()
	reader.add(sample(1))
	c := NewConsumer(b, m, ConsumerOptions{
		Buffer:         8,
		Replay:         reader,
		ReplayInterval: 5 * time.Millisecond,
	})
	stop := make(chan struct{})
	go c.Run(stop)
	defer func() { close(stop); c.Close() }()

	b.Publish(sample(1))
	if e := drainEvents(t, c.Events(), 1, time.Second)[0]; e.Kind != EventLive {
		t.Fatalf("first frame = %s, want live", e.Kind)
	}
	if _, err := c.Resubscribe(context.Background()); err != nil {
		t.Fatalf("resubscribe: %v", err)
	}
	// The reader still holds seq 1, which is already delivered; replays may
	// not produce a second frame for it nor a second monitor insertion.
	time.Sleep(50 * time.Millisecond)
	select {
	case e := <-c.Events():
		if e.Kind == EventLive || e.Kind == EventReplay {
			t.Fatalf("duplicate frame for seq 1: %+v", e)
		}
	default:
	}
	if got := m.Samples(); len(got) != 1 {
		t.Fatalf("monitor saw %d samples, want exactly 1", len(got))
	}
}

func TestConsumerCursorCatchupFromTrace(t *testing.T) {
	b := NewBroadcaster()
	m := NewMonitor()
	reader := newMemReader()
	for seq := int64(1); seq <= 3; seq++ {
		reader.add(sample(seq))
	}
	// Simulate a consumer joining after history exists: raise the
	// broadcaster watermark to 3 via a sacrificial subscription (seq 1
	// buffers, 2..3 drop), then remove it.
	warm := b.Subscribe(1)
	for seq := int64(1); seq <= 3; seq++ {
		b.Publish(sample(seq))
	}
	warm.Unsubscribe()

	from := Cursor{} // explicit cursor zero: replay retained history
	c := NewConsumer(b, m, ConsumerOptions{
		Buffer:         4,
		Replay:         reader,
		From:           &from,
		ReplayInterval: 5 * time.Millisecond,
		MaxReplay:      128,
	})
	stop := make(chan struct{})
	go c.Run(stop)
	defer func() { close(stop); c.Close() }()

	var replaySeqs []int64
	deadline := time.After(2 * time.Second)
	for len(replaySeqs) < 3 {
		select {
		case e := <-c.Events():
			if e.Kind == EventReplay {
				replaySeqs = append(replaySeqs, e.Sample.Seq)
			}
			if e.Kind == EventLive {
				t.Fatalf("historical seq %d arrived as live", e.Sample.Seq)
			}
		case <-deadline:
			t.Fatalf("catch-up replay = %v, want 1..3", replaySeqs)
		}
	}
	if replaySeqs[0] != 1 || replaySeqs[2] != 3 {
		t.Fatalf("replay order = %v, want 1,2,3", replaySeqs)
	}
	if got := m.Samples(); len(got) != 0 {
		t.Fatalf("monitor saw historical samples: %+v", got)
	}
}

func TestConsumerReconnectRotatesAndFillsGap(t *testing.T) {
	b := NewBroadcaster()
	m := NewMonitor()
	reader := newMemReader()
	for seq := int64(2); seq <= 4; seq++ {
		reader.add(sample(seq))
	}
	c := NewConsumer(b, m, ConsumerOptions{
		Buffer:         1,
		Events:         64,
		Replay:         reader,
		ReplayInterval: 5 * time.Millisecond,
		MaxReplay:      128,
	})
	stop := make(chan struct{})
	go c.Run(stop)
	defer func() { close(stop); c.Close() }()

	b.Publish(sample(1))
	drainEvents(t, c.Events(), 1, time.Second) // live 1

	// Transport drop, then seq 2..4 arrive while rotating.
	cursor, err := c.Resubscribe(context.Background())
	if err != nil {
		t.Fatalf("resubscribe: %v", err)
	}
	if cursor.Seq != 1 {
		t.Fatalf("reconnect cursor = %d, want 1", cursor.Seq)
	}
	for seq := int64(2); seq <= 4; seq++ {
		b.Publish(sample(seq))
	}

	frames := collectSampleFrames(t, c, 3, 2*time.Second)
	for seq := int64(2); seq <= 4; seq++ {
		if _, ok := frames[seq]; !ok {
			t.Fatalf("seq %d missing after reconnect", seq)
		}
	}
	if c.ReconnectCount() < 1 {
		t.Fatal("reconnect count did not advance")
	}
}

func TestConsumerExpiredGapStopsCatchup(t *testing.T) {
	b := NewBroadcaster()
	reader := newMemReader()
	reader.expired = 10
	from := Cursor{Seq: 5}
	c := NewConsumer(b, nil, ConsumerOptions{
		Buffer:         2,
		Replay:         reader,
		From:           &from,
		ReplayInterval: 5 * time.Millisecond,
	})
	stop := make(chan struct{})
	go c.Run(stop)
	defer func() { close(stop); c.Close() }()

	time.Sleep(50 * time.Millisecond)
	if cursor := c.Cursor().Seq; cursor != 5 {
		t.Fatalf("cursor advanced past expired gap: %d", cursor)
	}
}

func TestReconnectPolicyConnect(t *testing.T) {
	b := NewBroadcaster()
	c := NewConsumer(b, nil, ConsumerOptions{ReplayInterval: time.Millisecond})
	stop := make(chan struct{})
	go c.Run(stop)
	defer func() { close(stop); c.Close() }()

	policy := DefaultReconnectPolicy()
	cursor, err := policy.Connect(context.Background(), c)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if cursor.Seq != c.Cursor().Seq {
		t.Fatalf("returned cursor %d != consumer cursor %d", cursor.Seq, c.Cursor().Seq)
	}
	if c.ReconnectCount() != 1 {
		t.Fatalf("reconnects = %d, want 1", c.ReconnectCount())
	}
}
