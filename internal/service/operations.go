package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/wyw14/cry-161/internal/interlock"
	"github.com/wyw14/cry-161/internal/journal"
	"github.com/wyw14/cry-161/internal/model"
	"github.com/wyw14/cry-161/internal/optical"
	"github.com/wyw14/cry-161/internal/quartz"
	"github.com/wyw14/cry-161/internal/recipe"
	"github.com/wyw14/cry-161/internal/run"
	"github.com/wyw14/cry-161/internal/shutter"
	"github.com/wyw14/cry-161/internal/source"
	"github.com/wyw14/cry-161/internal/vacuum"
	"sync"
	"time"
)

type Runtime struct {
	mu              sync.RWMutex
	Monitor         *optical.Monitor
	Window          *optical.ThicknessWindow
	Broadcast       *optical.Broadcaster
	Traces          *journal.TraceStore
	Incidents       *journal.IncidentStore
	Timeline        *run.Timeline
	VacuumTelemetry *vacuum.Telemetry
	RevisionStore   *recipe.RevisionStore
	InterlockAudit  *interlock.Audit
	Shutter         *shutter.Controller
	Sequence        int64
	sampleSeq       int64
	StartedAt       time.Time
}

func NewRuntime() *Runtime {
	driver := shutter.NewDriver()
	feedback := shutter.NewFeedback()
	return &Runtime{Monitor: optical.NewMonitor(), Window: optical.NewThicknessWindow(128), Broadcast: optical.NewBroadcaster(), Traces: journal.NewTraceStore(), Incidents: journal.NewIncidentStore(), Timeline: run.NewTimeline(), VacuumTelemetry: vacuum.NewTelemetry(128), RevisionStore: recipe.NewRevisionStore(), InterlockAudit: interlock.NewAudit(), Shutter: shutter.NewController(driver, feedback), StartedAt: time.Now().UTC()}
}

func (r *Runtime) NextSequence() int64 {
	r.mu.Lock()
	r.Sequence++
	value := r.Sequence
	r.mu.Unlock()
	return value
}

// NextSampleSequence hands out the monotonic sequence carried by every
// thickness sample on both the live channel and the replay trace.
func (r *Runtime) NextSampleSequence() int64 {
	r.mu.Lock()
	r.sampleSeq++
	value := r.sampleSeq
	r.mu.Unlock()
	return value
}

// traceReplay adapts the trace store to optical.ReplayReader. An expired
// cursor is wrapped so it matches both journal.ErrCursorExpired and
// optical.ErrGapExpired.
type traceReplay struct{ traces *journal.TraceStore }

func (t traceReplay) ReplayAfter(ctx context.Context, cursor optical.Cursor, limit int) ([]model.ThicknessSample, error) {
	samples, err := t.traces.ReplayAfter(ctx, cursor.Seq, limit)
	if err != nil {
		if errors.Is(err, journal.ErrCursorExpired) {
			return nil, fmt.Errorf("%w: %w", optical.ErrGapExpired, err)
		}
		return nil, err
	}
	return samples, nil
}

// NewConsumer attaches an optical consumer to the runtime broadcaster. Gap
// replay is served from the trace store; replayed samples reach the
// consumer's event channel only and never re-enter the safety Monitor.
func (r *Runtime) NewConsumer(monitor *optical.Monitor, options optical.ConsumerOptions) *optical.Consumer {
	if options.Replay == nil {
		options.Replay = traceReplay{traces: r.Traces}
	}
	return optical.NewConsumer(r.Broadcast, monitor, options)
}

// Replay returns trace samples strictly after cursor without touching any
// live subscription or monitor.
func (r *Runtime) Replay(ctx context.Context, cursor optical.Cursor, limit int) (optical.ReplayReport, error) {
	return optical.ReplayGap(ctx, traceReplay{traces: r.Traces}, cursor, limit)
}

func (r *Runtime) Capture(sample model.ThicknessSample, layer int) (model.LayerEvidence, error) {
	if sample.At.IsZero() {
		sample.At = time.Now().UTC()
	}
	if sample.Seq == 0 {
		sample.Seq = r.NextSampleSequence()
	} else {
		// Honour caller-supplied sequences while keeping the counter ahead.
		r.mu.Lock()
		if sample.Seq > r.sampleSeq {
			r.sampleSeq = sample.Seq
		}
		r.mu.Unlock()
	}
	r.Monitor.Add(sample)
	r.Window.Add(sample)
	// Persist before publishing: a consumer dropping an older live sample
	// must be able to fill the gap from the trace immediately.
	r.Traces.AppendThickness(sample)
	_ = r.Broadcast.PublishThen(sample, nil)
	evidence := model.BuildLayerEvidence(sample, layer)
	if err := evidence.Validate(); err != nil {
		r.Incidents.Raise(sample.RunID, "warning", err.Error())
		return evidence, err
	}
	r.Timeline.Add(sample.RunID, "thickness", fmt.Sprintf("%.3f nm", sample.Nanometers))
	return evidence, nil
}

func (r *Runtime) RecordFrequency(runID model.ID, values []float64) {
	copied := append([]float64(nil), values...)
	r.Traces.AppendFrequency(runID, copied)
}

func (r *Runtime) Command(runID model.ID, revision int, kind model.CommandKind) (model.Command, error) {
	command := model.NewCommand(runID, revision, r.NextSequence(), kind)
	if err := command.Validate(); err != nil {
		return command.Fail(err), err
	}
	r.Timeline.Add(runID, "command", string(kind))
	return command.Complete("accepted"), nil
}

func (r *Runtime) Summary() map[string]any {
	latest := r.Monitor.Latest()
	return map[string]any{"started_at": r.StartedAt, "sequence": r.Sequence, "latest_thickness": latest, "trace_count": r.Traces.Count(), "timeline_entries": r.Timeline.Count(), "incidents": r.Incidents.List(), "vacuum": r.VacuumTelemetry.Readings(), "shutter_commands": r.Shutter.Commands()}
}

func (s *Service) RunLayer(ctx context.Context, runtime *Runtime, runID model.ID, layer model.Layer) error {
	value, ok := s.Runs.Get(runID)
	if !ok {
		return fmt.Errorf("run not found")
	}
	command, err := runtime.Command(runID, value.Layer, model.CommandCondition)
	if err != nil || command.Status != "completed" {
		return fmt.Errorf("layer command rejected")
	}
	worker := source.NewWorker(s.Source)
	if err := worker.Execute(ctx, layer); err != nil {
		incident := runtime.Incidents.Raise(runID, "critical", err.Error())
		_ = s.Journal.Append(journal.IncidentRaised(incident))
		s.Runs.UpdateError(runID, err)
		return err
	}
	profile := quartz.DefaultCalibration()
	sample := model.ThicknessSample{RunID: runID, Frequency: layer.TargetNanometers / profile.Slope, Transmission: 0.95, Nanometers: layer.TargetNanometers, Valid: true, At: time.Now().UTC()}
	if _, err := runtime.Capture(sample, layer.Number); err != nil {
		return err
	}
	return nil
}
