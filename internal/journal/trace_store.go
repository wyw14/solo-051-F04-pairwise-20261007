package journal

import (
	"context"
	"errors"
	"github.com/wyw14/cry-161/internal/model"
	"sync"
)

// DefaultReplayDepth is how many of the most recent thickness samples the
// trace store keeps available for gap replay, per process.
const DefaultReplayDepth = 4096

// ErrCursorExpired is returned when a replay cursor points before the
// oldest retained sample; the gap can no longer be filled from the trace.
var ErrCursorExpired = errors.New("journal: replay cursor expired")

type Trace struct {
	RunID       model.ID                `json:"run_id"`
	Frequencies []float64               `json:"frequencies"`
	Thickness   []model.ThicknessSample `json:"thickness"`
}

type TraceStore struct {
	mu     sync.RWMutex
	traces map[model.ID]Trace
	// ring is a bounded, seq-ordered log across all runs used for replay.
	ring []model.ThicknessSample
	head int
	size int
}

func NewTraceStore() *TraceStore {
	return NewTraceStoreWithDepth(DefaultReplayDepth)
}

func NewTraceStoreWithDepth(depth int) *TraceStore {
	if depth < 1 {
		depth = 1
	}
	return &TraceStore{traces: map[model.ID]Trace{}, ring: make([]model.ThicknessSample, depth)}
}

func (s *TraceStore) AppendFrequency(runID model.ID, values []float64) {
	s.mu.Lock()
	trace := s.traces[runID]
	trace.RunID = runID
	trace.Frequencies = append(trace.Frequencies, values...)
	s.traces[runID] = trace
	s.mu.Unlock()
}

func (s *TraceStore) AppendThickness(sample model.ThicknessSample) {
	s.mu.Lock()
	trace := s.traces[sample.RunID]
	trace.RunID = sample.RunID
	trace.Thickness = append(trace.Thickness, sample)
	s.traces[sample.RunID] = trace
	s.ring[s.head] = sample
	s.head = (s.head + 1) % len(s.ring)
	if s.size < len(s.ring) {
		s.size++
	}
	s.mu.Unlock()
}

func (s *TraceStore) Trace(runID model.ID) Trace {
	s.mu.RLock()
	defer s.mu.RUnlock()
	source := s.traces[runID]
	result := Trace{RunID: source.RunID}
	result.Frequencies = append([]float64(nil), source.Frequencies...)
	result.Thickness = append([]model.ThicknessSample(nil), source.Thickness...)
	return result
}

func (s *TraceStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.traces)
}

// ordered copies the replay ring in ascending sequence order. Callers must
// hold at least a read lock.
func (s *TraceStore) ordered() []model.ThicknessSample {
	out := make([]model.ThicknessSample, 0, s.size)
	if s.size < len(s.ring) {
		out = append(out, s.ring[:s.size]...)
		return out
	}
	out = append(out, s.ring[s.head:]...)
	out = append(out, s.ring[:s.head]...)
	return out
}

// OldestSequence returns the lowest sequence still retained for replay.
func (s *TraceStore) OldestSequence() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.size == 0 {
		return 0
	}
	return s.ordered()[0].Seq
}

// ReplayAfter returns up to limit retained samples whose sequence is strictly
// greater than cursor, in ascending sequence order across all runs. A cursor
// that has aged out of the bounded log yields ErrCursorExpired.
func (s *TraceStore) ReplayAfter(ctx context.Context, cursor int64, limit int) ([]model.ThicknessSample, error) {
	if limit < 1 {
		limit = 256
	}
	s.mu.RLock()
	ordered := s.ordered()
	s.mu.RUnlock()
	if len(ordered) == 0 {
		return []model.ThicknessSample{}, nil
	}
	if cursor > 0 && cursor < ordered[0].Seq-1 {
		return nil, ErrCursorExpired
	}
	first := 0
	for first < len(ordered) && ordered[first].Seq <= cursor {
		first++
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	end := first + limit
	if end > len(ordered) {
		end = len(ordered)
	}
	result := make([]model.ThicknessSample, end-first)
	copy(result, ordered[first:end])
	return result, nil
}
