package optical

import (
	"github.com/wyw14/cry-161/internal/model"
	"sync"
	"sync/atomic"
)

type subscriber struct {
	id        int
	channel   chan model.ThicknessSample
	dropped   atomic.Int64
	lastSeq   atomic.Int64
	startHigh int64
}

// Subscription is a live channel handle. The channel is dropped-to-oldest
// free: a full buffer never blocks the publisher; the dropped sample is
// counted instead so the consumer can detect and replay the gap.
type Subscription struct {
	sub *subscriber
	b   *Broadcaster
}

func (s *Subscription) ID() int                               { return s.sub.id }
func (s *Subscription) Channel() <-chan model.ThicknessSample { return s.sub.channel }
func (s *Subscription) Unsubscribe()                          { s.b.unsubscribe(s.sub.id) }

// StartHigh is the publisher's high watermark when this subscription was
// created: no sequence at or below it can ever arrive on this channel.
func (s *Subscription) StartHigh() int64 { return s.sub.startHigh }

// Status reports drops, the last delivered sequence and the reconnect cursor.
func (s *Subscription) Status() SubscriptionStatus {
	dropped := s.sub.dropped.Load()
	last := s.sub.lastSeq.Load()
	return SubscriptionStatus{
		Dropped:  dropped,
		LastSeq:  last,
		Cursor:   Cursor{Seq: last},
		Overflow: dropped > 0,
	}
}

type Broadcaster struct {
	mu          sync.RWMutex
	subscribers map[int]*subscriber
	next        int
	high        atomic.Int64
}

func NewBroadcaster() *Broadcaster {
	return &Broadcaster{subscribers: map[int]*subscriber{}}
}

// Subscribe opens a buffered live subscription. Buffers smaller than one are
// normalised to one.
func (b *Broadcaster) Subscribe(size int) *Subscription {
	if size < 1 {
		size = 1
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	sub := &subscriber{
		id:        b.next,
		channel:   make(chan model.ThicknessSample, size),
		startHigh: b.high.Load(),
	}
	b.next++
	b.subscribers[sub.id] = sub
	return &Subscription{sub: sub, b: b}
}

func (b *Broadcaster) unsubscribe(id int) {
	b.mu.Lock()
	sub, ok := b.subscribers[id]
	if ok {
		delete(b.subscribers, id)
		close(sub.channel)
	}
	b.mu.Unlock()
}

// Unsubscribe removes the subscription by id. Prefer Subscription.Unsubscribe.
func (b *Broadcaster) Unsubscribe(id int) { b.unsubscribe(id) }

// Publish fans a sample out to every live subscription. It never blocks:
// when a subscriber buffer is full the sample is dropped for that subscriber
// and counted, so slow real-time consumers cannot stall safety control.
// Publish takes the write lock so a gap probe observes a consistent
// (buffer-occupancy, high-watermark) pair.
func (b *Broadcaster) Publish(sample model.ThicknessSample) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, sub := range b.subscribers {
		select {
		case sub.channel <- sample:
			storeMaxSeq(&sub.lastSeq, sample.Seq)
		default:
			sub.dropped.Add(1)
		}
	}
	// Raised only after every send decision is settled: a probe that sees
	// an empty channel and this watermark can prove the skipped sequences
	// were dropped rather than still buffered.
	storeMaxSeq(&b.high, sample.Seq)
}

// DroppedGap atomically reports whether the named subscription has an empty
// channel while the publisher watermark is above floor. Both hold means the
// interval (floor, watermark] can never arrive on the live channel and must
// be replayed.
func (b *Broadcaster) DroppedGap(subscriptionID int, floor int64) (int64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	sub, ok := b.subscribers[subscriptionID]
	if !ok {
		return 0, false
	}
	high := b.high.Load()
	if len(sub.channel) == 0 && high > floor {
		return high, true
	}
	return 0, false
}

// storeMaxSeq atomically raises value to at least seq.
func storeMaxSeq(value *atomic.Int64, seq int64) {
	for {
		current := value.Load()
		if seq <= current || value.CompareAndSwap(current, seq) {
			return
		}
	}
}

// SubscriberCount reports active live subscriptions.
func (b *Broadcaster) SubscriberCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subscribers)
}

// LastSequence reports the highest sequence ever published; zero before the
// first sample.
func (b *Broadcaster) LastSequence() int64 { return b.high.Load() }
