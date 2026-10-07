package optical

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wyw14/cry-161/internal/model"
)

// ConsumerOptions configures a Consumer. Zero values fall back to defaults.
type ConsumerOptions struct {
	// Buffer is the live subscription channel capacity.
	Buffer int
	// Events is the presentation event channel capacity.
	Events int
	// Replay supplies gap samples from the trace. When nil, gaps are
	// reported via status events but cannot be filled.
	Replay ReplayReader
	// MaxReplay bounds a single replay batch.
	MaxReplay int
	// ReplayInterval is how often the consumer retries unfilled gaps.
	ReplayInterval time.Duration
	// From is the starting position. nil means "live from now on"; a
	// non-nil cursor (including zero) requests historical catch-up from the
	// trace strictly after that sequence.
	From *Cursor
}

// gapRange is a sequence interval (from, to] known to be absent from the
// live channel.
type gapRange struct {
	from int64
	to   int64
}

// Consumer joins a non-blocking live subscription with trace-backed gap
// replay. Two hard rules shape it:
//
//   - The real-time channel is never allowed to block safety control:
//     publisher drops are counted, and every hop here is best-effort.
//   - Replayed samples are emitted as EventReplay only. They never re-enter
//     the safety Monitor, and a sequence is delivered at most once across
//     the live and replay paths.
//
// Gap detection relies on the live channel being FIFO: when a live sample
// with sequence N arrives after sequence M (M < N-1), the sequences in
// (M, N) can never still arrive live, so they are replayed from the trace.
type Consumer struct {
	broadcaster *Broadcaster
	monitor     *Monitor
	reader      ReplayReader
	buffer      int
	maxReplay   int
	interval    time.Duration

	events chan Event

	mu            sync.Mutex
	sub           *Subscription
	cursor        int64
	stopped       bool
	reportDrops   int64
	reportPending bool
	pending       []gapRange
	catchingUp    bool
	catchupCursor int64
	catchupLimit  int64

	reconnects   atomic.Int64
	eventDropped atomic.Int64

	stop      chan struct{}
	control   chan resubscribeRequest
	kick      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup

	delivered *seqSet
}

type resubscribeRequest struct {
	ctx   context.Context
	reply chan Cursor
}

// NewConsumer wires a consumer to a broadcaster. monitor may be nil; when
// present it receives live samples only.
func NewConsumer(broadcaster *Broadcaster, monitor *Monitor, options ...ConsumerOptions) *Consumer {
	opts := ConsumerOptions{}
	if len(options) > 0 {
		opts = options[0]
	}
	if opts.Buffer < 1 {
		opts.Buffer = 8
	}
	if opts.Events < 1 {
		opts.Events = 256
	}
	if opts.MaxReplay < 1 {
		opts.MaxReplay = 256
	}
	if opts.ReplayInterval <= 0 {
		opts.ReplayInterval = 25 * time.Millisecond
	}
	c := &Consumer{
		broadcaster: broadcaster,
		monitor:     monitor,
		reader:      opts.Replay,
		buffer:      opts.Buffer,
		maxReplay:   opts.MaxReplay,
		interval:    opts.ReplayInterval,
		events:      make(chan Event, opts.Events),
		stop:        make(chan struct{}),
		control:     make(chan resubscribeRequest),
		kick:        make(chan struct{}, 1),
		delivered:   newSeqSet(opts.MaxReplay * 4),
	}
	// Subscribe up front so Status/Cursor are meaningful before Run. The
	// subscription captures the publisher high watermark at creation: no
	// sample at or below it can arrive on this channel.
	sub := broadcaster.Subscribe(opts.Buffer)
	c.sub = sub
	c.cursor = sub.StartHigh()
	if opts.From != nil {
		c.cursor = opts.From.Seq
		c.catchingUp = true
		c.catchupCursor = opts.From.Seq
		// Only sequences below the subscription's starting watermark are
		// guaranteed not to arrive live; catch-up is bounded by them.
		c.catchupLimit = sub.StartHigh()
	}
	return c
}

// Run pumps live samples and gap replay until stop fires or Close is called.
func (c *Consumer) Run(stop <-chan struct{}) {
	c.signalReplay()

	c.wg.Add(2)
	go c.runReplay()
	go c.runLive(c.sub)

	if stop != nil {
		select {
		case <-stop:
			c.Close()
		case <-c.stop:
		}
	} else {
		<-c.stop
	}
	c.wg.Wait()
}

func (c *Consumer) runLive(initial *Subscription) {
	defer c.wg.Done()
	sub := initial
	// lastLive is the highest sequence observed on the current channel.
	// Sequences skipped between two live deliveries are permanently absent
	// from this FIFO channel, hence safe replay candidates.
	lastLive := sub.StartHigh()
	probe := time.NewTicker(c.interval)
	defer probe.Stop()
	backoff := time.Millisecond
	for {
		ch := sub.Channel()
		select {
		case sample, ok := <-ch:
			if !ok {
				if c.isStopped() {
					return
				}
				// Defensive: an external unsubscribe looks like a dropped
				// transport. Rotate and let replay fill the gap.
				c.reconnects.Add(1)
				if !c.sleep(backoff) {
					return
				}
				if backoff < 50*time.Millisecond {
					backoff *= 2
				}
				lastLive, sub = c.rotate(sub, lastLive)
				continue
			}
			backoff = time.Millisecond
			if sample.Seq > lastLive+1 {
				c.queueGap(lastLive, sample.Seq)
			}
			lastLive = sample.Seq
			c.deliverLive(sample)
			c.pumpStatus()
		case <-probe.C:
			// Empty channel plus a higher publisher watermark proves the
			// interval was dropped; without a later live sample this probe
			// is the only way to notice a tail gap.
			if high, ok := c.broadcaster.DroppedGap(sub.ID(), lastLive); ok {
				c.queueGap(lastLive, high)
				lastLive = high
			}
		case req := <-c.control:
			sub.Unsubscribe()
			c.reconnects.Add(1)
			lastLive, sub = c.rotate(sub, lastLive)
			cursor := c.Cursor()
			select {
			case req.reply <- cursor:
			default:
			}
		case <-c.stop:
			sub.Unsubscribe()
			return
		}
	}
}

// rotate installs a fresh subscription and queues everything between the
// last consumed sequence and the new channel's starting high watermark.
func (c *Consumer) rotate(old *Subscription, lastLive int64) (int64, *Subscription) {
	sub := c.broadcaster.Subscribe(c.buffer)
	startHigh := sub.StartHigh()
	if startHigh > lastLive {
		c.queueGap(lastLive, startHigh)
	}
	c.mu.Lock()
	stopped := c.stopped
	if !stopped && c.sub == old {
		c.sub = sub
	}
	lastLive = startHigh
	c.mu.Unlock()
	if stopped {
		// Close already tore down the active subscription; do not leak
		// this freshly created one into the broadcaster.
		sub.Unsubscribe()
		return lastLive, sub
	}
	c.signalReplay()
	return lastLive, sub
}

func (c *Consumer) runReplay() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-c.kick:
			c.processGaps()
		case <-ticker.C:
			c.processGaps()
		}
	}
}

// processGaps drains historical catch-up first, then fills precisely known
// gap ranges. Reads come from the trace only and never touch the Monitor.
func (c *Consumer) processGaps() {
	if c.reader == nil {
		// Without a trace reader gaps cannot be filled; drop the bookkeeping
		// so it cannot accumulate. The overflow flag is still reported from
		// the cumulative drop counter.
		c.mu.Lock()
		c.pending = nil
		c.catchingUp = false
		c.mu.Unlock()
		c.pumpStatus()
		return
	}
	if c.isCatchingUp() {
		c.drainCatchup()
	}
	c.drainRanges()
	c.pumpStatus()
}

// drainCatchup walks retained history from the requested start cursor up to
// the subscription's starting watermark in batches. Sequences above that
// watermark may still arrive live, so they are never claimed here.
func (c *Consumer) drainCatchup() {
	cursor, limit := c.catchupState()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	samples, err := c.reader.ReplayAfter(ctx, Cursor{Seq: cursor}, c.maxReplay)
	cancel()
	if err != nil {
		if errors.Is(err, ErrGapExpired) {
			c.finishCatchup()
		}
		// Transient errors retry on the next tick.
		return
	}
	high := cursor
	exhausted := len(samples) < c.maxReplay
	for _, sample := range samples {
		if sample.Seq <= cursor {
			continue
		}
		if sample.Seq > limit {
			exhausted = true
			break
		}
		if c.delivered.claim(sample.Seq) {
			c.emit(Event{Kind: EventReplay, Sample: sample})
		}
		c.advance(sample.Seq)
		high = sample.Seq
	}
	if exhausted {
		c.finishCatchup()
		return
	}
	c.advanceCatchup(high)
}

func (c *Consumer) finishCatchup() {
	c.mu.Lock()
	c.catchingUp = false
	c.mu.Unlock()
}

// drainRanges fills each known gap interval, requeuing the remainder when
// the trace does not yet contain every missing sequence.
func (c *Consumer) drainRanges() {
	for {
		c.mu.Lock()
		if len(c.pending) == 0 {
			c.mu.Unlock()
			return
		}
		r := c.pending[0]
		c.pending = c.pending[1:]
		c.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		samples, err := c.reader.ReplayAfter(ctx, Cursor{Seq: r.from}, c.maxReplay)
		cancel()
		if err != nil {
			if errors.Is(err, ErrGapExpired) {
				// This interval is permanently unrecoverable; move on to
				// the next queued range rather than blocking the queue.
				continue
			}
			// Transient: put it back for the next tick.
			c.requeue(r)
			return
		}
		high := r.from
		for _, sample := range samples {
			if sample.Seq <= r.from {
				continue
			}
			if sample.Seq > r.to {
				break
			}
			if c.delivered.claim(sample.Seq) {
				c.emit(Event{Kind: EventReplay, Sample: sample})
			}
			c.advance(sample.Seq)
			high = sample.Seq
		}
		if high < r.to {
			// Some missing sequences are not in the trace yet (or the
			// batch was truncated); keep trying from the last seen point.
			c.requeue(gapRange{from: high, to: r.to})
		}
	}
}

// deliverLive feeds the safety sink first, independently of event delivery,
// then presents the sample. A sequence already supplied by replay is skipped
// entirely so the Monitor never sees a sample twice.
func (c *Consumer) deliverLive(sample model.ThicknessSample) {
	if sample.Seq > 0 && !c.delivered.claim(sample.Seq) {
		c.advance(sample.Seq)
		return
	}
	if c.monitor != nil {
		c.monitor.Add(sample)
	}
	if sample.Seq > 0 {
		c.advance(sample.Seq)
	}
	c.emit(Event{Kind: EventLive, Sample: sample})
}

// pumpStatus emits a status frame only when the gap/drop picture changed.
func (c *Consumer) pumpStatus() {
	status := c.subscriptionStatus()
	c.mu.Lock()
	changed := status.Dropped > c.reportDrops || status.Overflow != c.reportPending
	if changed {
		c.reportDrops = status.Dropped
		c.reportPending = status.Overflow
	}
	c.mu.Unlock()
	if changed {
		c.emit(Event{Kind: EventStatus, Status: status})
	}
}

func (c *Consumer) emit(event Event) {
	select {
	case c.events <- event:
	default:
		c.eventDropped.Add(1)
	}
}

func (c *Consumer) signalReplay() {
	select {
	case c.kick <- struct{}{}:
	default:
	}
}

func (c *Consumer) advance(seq int64) {
	c.mu.Lock()
	if seq > c.cursor {
		c.cursor = seq
	}
	c.mu.Unlock()
}

func (c *Consumer) queueGap(from, to int64) {
	if to <= from {
		return
	}
	c.mu.Lock()
	c.pending = append(c.pending, gapRange{from: from, to: to})
	c.mu.Unlock()
	c.signalReplay()
}

func (c *Consumer) requeue(r gapRange) {
	if r.to <= r.from {
		return
	}
	c.mu.Lock()
	c.pending = append([]gapRange{r}, c.pending...)
	c.mu.Unlock()
}

func (c *Consumer) isCatchingUp() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.catchingUp
}

func (c *Consumer) catchupState() (cursor, limit int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.catchupCursor, c.catchupLimit
}

func (c *Consumer) advanceCatchup(seq int64) {
	c.mu.Lock()
	if seq > c.catchupCursor {
		c.catchupCursor = seq
	}
	c.mu.Unlock()
}

// Cursor reports the highest sequence delivered over either path.
func (c *Consumer) Cursor() Cursor {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Cursor{Seq: c.cursor}
}

// Events exposes live, replay and status frames. The channel is never
// closed; use Close to stop processing.
func (c *Consumer) Events() <-chan Event { return c.events }

// Resubscribe rotates the live transport as if the connection had dropped,
// then fills the resulting gap from the trace.
func (c *Consumer) Resubscribe(ctx context.Context) (Cursor, error) {
	reply := make(chan Cursor, 1)
	req := resubscribeRequest{ctx: ctx, reply: reply}
	select {
	case c.control <- req:
	case <-c.stop:
		return Cursor{}, ErrConsumerStopped
	case <-ctx.Done():
		return Cursor{}, ctx.Err()
	}
	select {
	case cursor := <-reply:
		return cursor, nil
	case <-c.stop:
		return Cursor{}, ErrConsumerStopped
	case <-ctx.Done():
		return Cursor{}, ctx.Err()
	}
}

// Close stops both loops. Safe to call multiple times.
func (c *Consumer) Close() {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.stopped = true
		if c.sub != nil {
			c.sub.Unsubscribe()
		}
		c.mu.Unlock()
		close(c.stop)
	})
}

func (c *Consumer) isStopped() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stopped
}

func (c *Consumer) sleep(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-c.stop:
		return false
	}
}

func (c *Consumer) subscriptionStatus() SubscriptionStatus {
	c.mu.Lock()
	sub := c.sub
	cursor := c.cursor
	pending := len(c.pending) > 0 || c.catchingUp
	c.mu.Unlock()
	status := SubscriptionStatus{Cursor: Cursor{Seq: cursor}, Overflow: pending}
	if sub != nil {
		live := sub.Status()
		status.Dropped = live.Dropped
		status.LastSeq = live.LastSeq
	}
	// Without a reader every cumulative drop is a permanent overflow.
	if c.reader == nil && status.Dropped > 0 {
		status.Overflow = true
	}
	return status
}

// Status returns dropped count, last live sequence, the reconnect cursor and
// the overflow flag (true while a gap is still being filled).
func (c *Consumer) Status() SubscriptionStatus { return c.subscriptionStatus() }

// ReconnectCount reports transport rotations observed since start.
func (c *Consumer) ReconnectCount() int { return int(c.reconnects.Load()) }

// DroppedEvents reports presentation frames that could not be queued.
func (c *Consumer) DroppedEvents() int64 { return c.eventDropped.Load() }

// WaitForSample blocks until the safety Monitor has a sample or the timeout.
func (c *Consumer) WaitForSample(timeout time.Duration) (model.ThicknessSample, bool) {
	if c.monitor == nil {
		return model.ThicknessSample{}, false
	}
	deadline := time.Now().Add(timeout)
	for {
		sample := c.monitor.Latest()
		if !sample.At.IsZero() {
			return sample, true
		}
		if time.Now().After(deadline) {
			return model.ThicknessSample{}, false
		}
		time.Sleep(time.Millisecond)
	}
}

// seqSet is a bounded record of sequences already delivered, shared by the
// live and replay paths so a sequence cannot be emitted twice.
type seqSet struct {
	mu     sync.Mutex
	values map[int64]struct{}
	keep   int
}

func newSeqSet(keep int) *seqSet {
	if keep < 64 {
		keep = 64
	}
	return &seqSet{values: make(map[int64]struct{}, keep), keep: keep}
}

func (s *seqSet) claim(seq int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.values[seq]; ok {
		return false
	}
	s.values[seq] = struct{}{}
	if len(s.values) > s.keep*2 {
		floor := seq - int64(s.keep)
		for value := range s.values {
			if value < floor {
				delete(s.values, value)
			}
		}
	}
	return true
}
