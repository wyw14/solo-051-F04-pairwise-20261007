package optical

import (
	"github.com/wyw14/cry-161/internal/model"
	"sync"
)

// SubscriptionStats is the consumer-visible state of one subscription. It is
// returned by Subscribe and can be refreshed from Status.
type SubscriptionStats struct {
	ID            int   `json:"id"`
	LastSeq       int64 `json:"last_seq"`
	Dropped       int64 `json:"dropped"`
	LastDelivered int64 `json:"last_delivered"`
}

// subscriber is tracked per channel. Dropped counts samples that could not be
// handed off because the consumer's buffer was full; LastSeq is the most recent
// sequence the broadcaster has published to this subscription (delivered or
// not). Together they let a reconnecting consumer ask the trace for the gap.
type subscriber struct {
	ch            chan model.ThicknessSample
	lastSeq       int64
	lastDelivered int64
	dropped       int64
}

type Broadcaster struct {
	mu          sync.RWMutex
	subscribers map[int]*subscriber
	next        int
}

func NewBroadcaster() *Broadcaster {
	return &Broadcaster{subscribers: map[int]*subscriber{}}
}

// Subscribe opens a non-blocking subscription. The returned stats are a point
// in time; call Status(id) to read refreshed dropped/last-sequence values.
func (b *Broadcaster) Subscribe(size int) (int, <-chan model.ThicknessSample, SubscriptionStats) {
	if size < 1 {
		size = 1
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	id := b.next
	b.next++
	sub := &subscriber{ch: make(chan model.ThicknessSample, size)}
	b.subscribers[id] = sub
	return id, sub.ch, SubscriptionStats{ID: id}
}

func (b *Broadcaster) Unsubscribe(id int) {
	b.mu.Lock()
	if sub, ok := b.subscribers[id]; ok {
		delete(b.subscribers, id)
		close(sub.ch)
	}
	b.mu.Unlock()
}

// Publish hands a sample to every subscriber. A full per-subscriber buffer is
// never blocked on: the sample is counted as dropped for that subscriber and
// the loop moves on, so a slow realtime consumer cannot stall the publisher or
// the safety controls running behind it.
func (b *Broadcaster) Publish(sample model.ThicknessSample) {
	b.mu.RLock()
	subs := make([]*subscriber, 0, len(b.subscribers))
	for _, sub := range b.subscribers {
		subs = append(subs, sub)
	}
	b.mu.RUnlock()
	for _, sub := range subs {
		select {
		case sub.ch <- sample:
			b.mu.Lock()
			sub.lastSeq = sample.Seq
			sub.lastDelivered = sample.Seq
			b.mu.Unlock()
		default:
			// Buffer full: drop for this subscriber rather than block the
			// realtime path. The consumer replays the gap from the trace.
			b.mu.Lock()
			sub.lastSeq = sample.Seq
			sub.dropped++
			b.mu.Unlock()
		}
	}
}

// Status returns a subscription's current counters. The boolean is false once
// the subscription has been removed.
func (b *Broadcaster) Status(id int) (SubscriptionStats, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	sub, ok := b.subscribers[id]
	if !ok {
		return SubscriptionStats{ID: id}, false
	}
	return SubscriptionStats{ID: id, LastSeq: sub.lastSeq, Dropped: sub.dropped, LastDelivered: sub.lastDelivered}, true
}
