package optical

import (
	"github.com/wyw14/cry-161/internal/model"
	"sync"
	"sync/atomic"
	"time"
)

// replayBuffer bounds how many replayed samples the recovery path can queue
// ahead of the gap reader. It is deliberately separate from the live channel so
// replay backpressure can never stall the realtime monitor path.
const replayBuffer = 64

// Cursor is the consumer's position in the global sample sequence. LastSeq is
// the contiguous frontier: every sample with seq <= LastSeq has been handled
// (live or replayed), and seq == LastSeq+1 is the first still-missing one. The
// frontier — not the high-water mark — is the reconnect cursor, because live
// delivery can overtake a dropped hole (seq 3 live while seq 1 is missing); a
// high-water cursor would hide that hole from the trace forever.
type Cursor struct {
	LastSeq    int64 `json:"last_seq"`
	Reconnects int   `json:"reconnects"`
}

// GapSource is the replay side of the trace log. *journal.TraceStore satisfies
// it; the optical package depends on the interface rather than the store so
// the replay contract stays local to the subscription protocol.
type GapSource interface {
	ReplayAfter(afterSeq int64) []model.ThicknessSample
}

type Consumer struct {
	mu         sync.Mutex
	samples    chan model.ThicknessSample
	replayIn   chan model.ThicknessSample
	gap        chan model.ThicknessSample
	monitor    *Monitor
	reconnects int

	// Coverage bookkeeping. covered holds every sequence the consumer has
	// handled on either path and is shared by the live and replay loops. It
	// drives both the contiguous frontier and exactly-once dedup:
	//   - a replay whose seq is already covered is suppressed (it was either
	//     delivered live in the overlap window or emitted by an earlier
	//     replay), so a gap sample is never processed twice;
	//   - the frontier advances only through a complete prefix, so the
	//     reconnect cursor can never skip a dropped hole.
	progMu   sync.Mutex
	covered  map[int64]struct{}
	frontier int64

	gapSkipped atomic.Int64

	liveWG   sync.WaitGroup
	replayWG sync.WaitGroup
}

func NewConsumer(monitor *Monitor, samples chan model.ThicknessSample) *Consumer {
	if samples == nil {
		samples = make(chan model.ThicknessSample, 8)
	}
	return &Consumer{
		monitor:  monitor,
		samples:  samples,
		replayIn: make(chan model.ThicknessSample, replayBuffer),
		gap:      make(chan model.ThicknessSample, replayBuffer),
		covered:  map[int64]struct{}{},
	}
}

// Run drains the live and replay channels until stop is closed. The two paths
// run on independent goroutines: a stalled gap reader can block replay, but it
// can never block the live samples feeding the safety monitor.
func (c *Consumer) Run(stop <-chan struct{}) {
	c.liveWG.Add(1)
	go c.drainLive(stop)
	c.replayWG.Add(1)
	go c.drainReplay(stop)
	c.liveWG.Wait()
	c.replayWG.Wait()
}

func (c *Consumer) drainLive(stop <-chan struct{}) {
	defer c.liveWG.Done()
	for {
		c.mu.Lock()
		samples := c.samples
		c.mu.Unlock()
		if samples == nil {
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
				continue
			}
		}
		select {
		case sample, ok := <-samples:
			if !ok {
				c.mu.Lock()
				if c.samples == samples {
					c.samples = nil
				}
				c.mu.Unlock()
				continue
			}
			// Only the realtime stream ever enters the safety monitor.
			c.monitor.Add(sample)
			if sample.Seq > 0 {
				c.cover(sample.Seq)
			}
		case <-stop:
			return
		}
	}
}

func (c *Consumer) drainReplay(stop <-chan struct{}) {
	defer c.replayWG.Done()
	for {
		select {
		case sample, ok := <-c.replayIn:
			if !ok {
				return
			}
			if !c.claim(sample.Seq) {
				c.gapSkipped.Add(1)
				continue
			}
			// Replayed samples leave for the gap reader; they never reach the
			// monitor.
			select {
			case c.gap <- sample:
			case <-stop:
				return
			}
		case <-stop:
			return
		}
	}
}

// cover records a live-delivered sequence and advances the frontier over any
// complete prefix.
func (c *Consumer) cover(seq int64) {
	c.progMu.Lock()
	defer c.progMu.Unlock()
	c.covered[seq] = struct{}{}
	c.advanceFrontierLocked()
}

// claim reserves a replayed sequence. It returns false when the sequence is
// already covered, so duplicates are dropped before the gap reader can see
// them. A successful claim advances the frontier just like live delivery.
func (c *Consumer) claim(seq int64) bool {
	c.progMu.Lock()
	defer c.progMu.Unlock()
	if seq <= 0 {
		return true
	}
	// At or below the contiguous frontier: handled on an earlier pass (the
	// coverage map only retains seqs above the frontier).
	if seq <= c.frontier {
		return false
	}
	if _, ok := c.covered[seq]; ok {
		return false
	}
	c.covered[seq] = struct{}{}
	c.advanceFrontierLocked()
	return true
}

func (c *Consumer) advanceFrontierLocked() {
	for {
		if _, ok := c.covered[c.frontier+1]; !ok {
			return
		}
		c.frontier++
		// The prefix below the frontier can never be queried or replayed
		// again, so drop it to keep the coverage set bounded.
		delete(c.covered, c.frontier)
	}
}

// Reconnect installs a fresh live channel and returns it together with the
// cursor at the current contiguous frontier. Pump the trace tail through
// ReplayGap (or ReplayAfter(cursor.LastSeq)) to recover the missing samples;
// overlaps with new live delivery are deduped automatically.
func (c *Consumer) Reconnect() (chan model.ThicknessSample, Cursor) {
	c.mu.Lock()
	defer c.mu.Unlock()
	samples := make(chan model.ThicknessSample, 8)
	c.samples = samples
	c.reconnects++
	return samples, Cursor{LastSeq: c.Frontier(), Reconnects: c.reconnects}
}

// ReplayIn is the sink for manual trace replay. It is drained by the replay
// loop only and must never be fed by the realtime publisher.
func (c *Consumer) ReplayIn() chan<- model.ThicknessSample { return c.replayIn }

// Gap is the source of deduped replayed samples. Nothing on this channel has
// touched the monitor; gap consumers (UI backfill, audit) own these samples.
func (c *Consumer) Gap() <-chan model.ThicknessSample { return c.gap }

// ReplayGap loads the missing tail of the trace (everything strictly after the
// current frontier) and enqueues it for replay. Dedup is applied authoritatively
// by the drain loop against live coverage, so a replay overlapping fresh live
// delivery (or a repeated ReplayGap) never double-emits. Returns the number of
// trace samples enqueued. This is a recovery pump: it may block on the replay
// buffer, but it runs off the realtime path, so it cannot stall the monitor.
func (c *Consumer) ReplayGap(source GapSource) int {
	if source == nil {
		return 0
	}
	gap := source.ReplayAfter(c.Frontier())
	for _, sample := range gap {
		c.replayIn <- sample
	}
	return len(gap)
}

// Frontier returns the contiguous cursor position.
func (c *Consumer) Frontier() int64 {
	c.progMu.Lock()
	defer c.progMu.Unlock()
	return c.frontier
}

func (c *Consumer) Position() Cursor {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Cursor{LastSeq: c.Frontier(), Reconnects: c.reconnects}
}

func (c *Consumer) ReconnectCount() int { c.mu.Lock(); defer c.mu.Unlock(); return c.reconnects }

// GapSkipped is the number of replayed samples suppressed because their
// sequences were already covered.
func (c *Consumer) GapSkipped() int64 { return c.gapSkipped.Load() }

func (c *Consumer) WaitForSample(timeout time.Duration) (model.ThicknessSample, bool) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		sample := c.monitor.Latest()
		if sample.At.IsZero() {
			select {
			case <-deadline.C:
				return model.ThicknessSample{}, false
			default:
				time.Sleep(time.Millisecond)
				continue
			}
		}
		return sample, true
	}
}
