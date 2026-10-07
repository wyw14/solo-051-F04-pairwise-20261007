package journal

import (
	"github.com/wyw14/cry-161/internal/model"
	"sort"
	"sync"
)

type Trace struct {
	RunID       model.ID                `json:"run_id"`
	Frequencies []float64               `json:"frequencies"`
	Thickness   []model.ThicknessSample `json:"thickness"`
}

type TraceStore struct {
	mu     sync.RWMutex
	traces map[model.ID]Trace
	seq    int64
}

func NewTraceStore() *TraceStore {
	return &TraceStore{traces: map[model.ID]Trace{}}
}

func (s *TraceStore) AppendFrequency(runID model.ID, values []float64) {
	s.mu.Lock()
	trace := s.traces[runID]
	trace.RunID = runID
	trace.Frequencies = append(trace.Frequencies, values...)
	s.traces[runID] = trace
	s.mu.Unlock()
}

// AppendThickness stores the sample and returns its sequence number. The store
// owns the global monotonic sample sequence: a sample without a sequence is
// assigned the next one before it is logged. Because the trace is the authority
// on the sequence, callers must append before publishing the returned sample so
// a reconnect cursor can always be satisfied from the log.
func (s *TraceStore) AppendThickness(sample model.ThicknessSample) int64 {
	s.mu.Lock()
	if sample.Seq == 0 {
		s.seq++
		sample.Seq = s.seq
	} else if sample.Seq > s.seq {
		s.seq = sample.Seq
	}
	trace := s.traces[sample.RunID]
	trace.RunID = sample.RunID
	trace.Thickness = append(trace.Thickness, sample)
	s.traces[sample.RunID] = trace
	seq := sample.Seq
	s.mu.Unlock()
	return seq
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

// Replay returns the stored samples of one run whose sequence is strictly
// greater than afterSeq, ordered by sequence. afterSeq is a reconnect cursor:
// pass the last sequence the consumer has already handled to receive only the
// missing tail of that run's trace.
func (s *TraceStore) Replay(runID model.ID, afterSeq int64) []model.ThicknessSample {
	s.mu.RLock()
	defer s.mu.RUnlock()
	source := s.traces[runID]
	gap := make([]model.ThicknessSample, 0, len(source.Thickness))
	for _, sample := range source.Thickness {
		if sample.Seq > afterSeq {
			gap = append(gap, sample)
		}
	}
	sort.Slice(gap, func(i, j int) bool { return gap[i].Seq < gap[j].Seq })
	return gap
}

// ReplayAfter returns missing samples across all runs (global sequence space),
// ordered by sequence. It backs reconnects for subscriptions observing the
// mixed-run realtime stream.
func (s *TraceStore) ReplayAfter(afterSeq int64) []model.ThicknessSample {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var gap []model.ThicknessSample
	for _, trace := range s.traces {
		for _, sample := range trace.Thickness {
			if sample.Seq > afterSeq {
				gap = append(gap, sample)
			}
		}
	}
	sort.Slice(gap, func(i, j int) bool { return gap[i].Seq < gap[j].Seq })
	return gap
}

// LastSequence is the highest sequence the log has assigned — the freshest
// cursor a fully-caught-up consumer can hold.
func (s *TraceStore) LastSequence() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.seq
}
