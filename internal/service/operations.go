package service

import (
	"context"
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

func (r *Runtime) Capture(sample model.ThicknessSample, layer int) (model.LayerEvidence, error) {
	if sample.At.IsZero() {
		sample.At = time.Now().UTC()
	}
	// The trace owns the sequence: number the sample in the log first so every
	// downstream copy (monitor, broadcast) carries the same seq and a
	// reconnect cursor can always resolve its gap from the trace.
	sample.Seq = r.Traces.AppendThickness(sample)
	r.Monitor.Add(sample)
	r.Window.Add(sample)
	// Non-blocking fan-out: a slow subscriber drops and later replays; the
	// safety-control path (monitor/evidence above) is never held behind it.
	r.Broadcast.Publish(sample)
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
