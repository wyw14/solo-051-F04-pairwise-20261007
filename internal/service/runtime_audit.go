package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/wyw14/cry-161/internal/chamber"
	"github.com/wyw14/cry-161/internal/journal"
	"github.com/wyw14/cry-161/internal/model"
	"github.com/wyw14/cry-161/internal/optical"
	"github.com/wyw14/cry-161/internal/quartz"
	"github.com/wyw14/cry-161/internal/recipe"
	"github.com/wyw14/cry-161/internal/rotation"
	"github.com/wyw14/cry-161/internal/run"
	"github.com/wyw14/cry-161/internal/shutter"
	"github.com/wyw14/cry-161/internal/source"
	"github.com/wyw14/cry-161/internal/substrate"
	"github.com/wyw14/cry-161/internal/vacuum"
	"time"
)

type AuditReport struct {
	Passed bool              `json:"passed"`
	Checks map[string]string `json:"checks"`
	At     time.Time         `json:"at"`
}

func (s *Service) Audit(ctx context.Context, runtime *Runtime) AuditReport {
	report := AuditReport{Passed: true, Checks: map[string]string{}, At: time.Now().UTC()}
	profile := vacuum.ProductionProfile()
	if err := profile.Validate(); err != nil {
		report.Passed = false
		report.Checks["vacuum_profile"] = err.Error()
	} else {
		report.Checks["vacuum_profile"] = profile.TotalTimeout().String()
	}
	snapshots := chamber.NewRegistry(s.Chamber).Snapshots()
	if len(snapshots) != 1 {
		report.Passed = false
		report.Checks["chambers"] = "unexpected chamber count"
	} else {
		copied := chamber.CopySnapshot(snapshots[0])
		merged := chamber.MergeSnapshots(copied, model.ChamberSnapshot{Name: "fallback"})
		report.Checks["chambers"] = merged.Name
	}
	recipes := s.Recipes.List()
	if len(recipes) == 0 {
		report.Passed = false
		report.Checks["recipes"] = "no recipes"
	} else {
		frozen, err := runtime.RevisionStore.Freeze(recipes[0])
		if err != nil && len(runtime.RevisionStore.List(recipes[0].ID)) == 0 {
			report.Passed = false
			report.Checks["recipes"] = err.Error()
		} else {
			plan, compileErr := recipe.Compile(recipes[0])
			if compileErr != nil {
				report.Passed = false
				report.Checks["recipes"] = compileErr.Error()
			} else {
				report.Checks["recipes"] = fmt.Sprintf("revision=%d layers=%d", frozen.Number, len(plan.Layers))
			}
		}
	}
	calibration := quartz.DefaultCalibration()
	estimate := calibration.Thickness(10)
	report.Checks["quartz"] = fmt.Sprintf("%.2f", estimate)
	record := journal.SpectrumRecord{Wavelength: []float64{450, 550, 650}, Intensity: []float64{0.9, 0.92, 0.91}}
	data, _ := json.Marshal(record)
	decoded, readErr := journal.ReadSpectrum(bytes.NewReader(append(data, byte(10))))
	if readErr != nil {
		report.Passed = false
		report.Checks["spectrum"] = readErr.Error()
	} else {
		score, analyzeErr := optical.Analyze(decoded)
		if analyzeErr != nil {
			report.Passed = false
			report.Checks["spectrum"] = analyzeErr.Error()
		} else {
			report.Checks["spectrum"] = fmt.Sprintf("%.3f", score)
		}
	}
	receiver := quartz.NewReceiver()
	blocks := receiver.Subscribe()
	receiver.Receive([]float64{10, 11, 12})
	block := <-blocks
	runtime.RecordFrequency(model.NewID(), block)
	monitor := optical.NewMonitor()
	samples := make(chan model.ThicknessSample, 2)
	consumer := optical.NewConsumer(monitor, samples)
	stop := make(chan struct{})
	go consumer.Run(stop)
	reconnect := optical.DefaultReconnectPolicy()
	connected, reconnectErr := reconnect.Connect(ctx, consumer)
	if reconnectErr != nil {
		report.Passed = false
		report.Checks["optical_reconnect"] = reconnectErr.Error()
	} else {
		connected.Channel <- model.ThicknessSample{RunID: model.NewID(), Seq: 1, Frequency: 10, Transmission: 0.9, Nanometers: 5, Valid: true, At: time.Now().UTC()}
		_, observed := consumer.WaitForSample(20 * time.Millisecond)
		report.Checks["optical_reconnect"] = fmt.Sprintf("observed=%t reconnects=%d cursor=%d", observed, consumer.ReconnectCount(), connected.Cursor.LastSeq)
	}

	// Subscription protocol: dropped counters, last sequence and trace replay
	// from a reconnect cursor. A size-1 subscriber is flooded; the broadcaster
	// must count the drops without blocking the publisher, the trace must
	// replay the gap by cursor, and replayed samples must never reach the
	// safety monitor a second time.
	subID, subCh, subStart := runtime.Broadcast.Subscribe(1)
	runID := model.NewID()
	for i := int64(1); i <= 4; i++ {
		runtime.Capture(model.ThicknessSample{RunID: runID, Frequency: 10, Transmission: 0.9, Nanometers: float64(i), Valid: true, At: time.Now().UTC()}, 1)
	}
	// Drain whatever the size-1 buffer currently holds so Status reflects a
	// settled subscriber, then read the counters.
drainLoop:
	for {
		select {
		case <-subCh:
		default:
			break drainLoop
		}
	}
	subStatus, subKnown := runtime.Broadcast.Status(subID)
	runtime.Broadcast.Unsubscribe(subID)
	// Replay the gap from the cursor the subscription reports.
	replayed := runtime.Traces.Replay(runID, subStatus.LastDelivered)
	report.Checks["optical_subscription"] = fmt.Sprintf(
		"known=%t start=%d last=%d dropped=%d replayed=%d trace_seq=%d",
		subKnown, subStart.LastSeq, subStatus.LastSeq, subStatus.Dropped, len(replayed), runtime.Traces.LastSequence())
	if !subKnown || subStatus.LastSeq != runtime.Traces.LastSequence() || subStatus.Dropped == 0 || len(replayed) == 0 {
		report.Passed = false
	}
	// Consumer-side cursor replay: the monitor must observe live samples only.
	gapMonitor := optical.NewMonitor()
	gapConsumer := optical.NewConsumer(gapMonitor, make(chan model.ThicknessSample, 8))
	gapStop := make(chan struct{})
	go gapConsumer.Run(gapStop)
	enqueued := gapConsumer.ReplayGap(runtime.Traces)
	received := 0
	gapDeadline := time.After(50 * time.Millisecond)
gapLoop:
	for received < enqueued {
		select {
		case <-gapConsumer.Gap():
			received++
		case <-gapDeadline:
			break gapLoop
		}
	}
	close(gapStop)
	close(stop)
	if !gapMonitor.Latest().At.IsZero() || received != enqueued || enqueued == 0 {
		report.Passed = false
		report.Checks["optical_gap_replay"] = fmt.Sprintf("enqueued=%d received=%d monitor_polluted=true skipped=%d", enqueued, received, gapConsumer.GapSkipped())
	} else {
		report.Checks["optical_gap_replay"] = fmt.Sprintf("enqueued=%d received=%d cursor=%d", enqueued, received, gapConsumer.Frontier())
	}
	carrier := substrate.NewCarrier()
	carrierRun := model.NewID()
	if err := carrier.Load("audit", carrierRun); err != nil {
		report.Passed = false
		report.Checks["carrier"] = err.Error()
	} else {
		found, ok := carrier.Run("audit")
		inspection := substrate.Inspect(model.Run{ID: found, Batch: "audit", RecipeID: model.NewID(), State: model.Loading})
		report.Checks["carrier"] = fmt.Sprintf("loaded=%t run=%t releaseable=%t", carrier.Loaded(), ok, inspection.Releaseable())
		carrier.Unload("audit")
	}
	rotationProfile := rotation.ProfileFor(model.Layer{Number: 1, TargetNanometers: 50, RateNanometers: 2})
	s.Rotation.Start(model.NewID(), rotationProfile.RPM)
	status := s.Rotation.Status()
	s.Rotation.Stop()
	report.Checks["rotation"] = fmt.Sprintf("valid=%t rpm=%v", rotationProfile.Valid(), status["rpm"])
	driver := shutter.NewDriver()
	feedback := shutter.NewFeedback()
	shutterController := shutter.NewController(driver, feedback)
	if err := shutterController.Open(ctx); err == nil {
		_ = shutterController.Close(ctx)
	}
	state, changed := feedback.State()
	report.Checks["shutter"] = fmt.Sprintf("state=%s at=%s", state, changed.Format(time.RFC3339Nano))
	admission := source.NewAdmission(10)
	if err := admission.Acquire(ctx, 2); err == nil {
		admission.Release(2)
	}
	report.Checks["source_admission"] = fmt.Sprintf("used=%d", admission.Used())
	starter := run.Starter{Registry: s.Runs, Vacuum: s.Vacuum}
	_ = starter
	report.Checks["run_registry"] = fmt.Sprintf("runs=%d", len(s.Runs.List()))
	runtime.VacuumTelemetry.Record(s.Vacuum.Pressure(), true)
	report.Checks["vacuum_telemetry"] = fmt.Sprintf("stable=%t", runtime.VacuumTelemetry.Stable(2, 1))
	cooldown := chamber.DefaultCooldownPolicy()
	s.Chamber.SetSourcePower(0)
	if err := cooldown.Wait(ctx, s.Chamber); err != nil {
		report.Passed = false
		report.Checks["cooldown"] = err.Error()
	} else {
		report.Checks["cooldown"] = "ready"
	}
	sourceCooldown := source.NewCooldown(s.Source, time.Millisecond)
	if err := sourceCooldown.Run(ctx); err != nil {
		report.Passed = false
		report.Checks["source_cooldown"] = err.Error()
	} else {
		report.Checks["source_cooldown"] = "ready"
	}
	value := model.Run{ID: model.NewID(), Batch: "audit", RecipeID: model.NewID(), State: model.Loading, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	if err := model.ValidateRun(value); err != nil {
		report.Passed = false
		report.Checks["model"] = err.Error()
	} else {
		result := run.BuildResult(&value, false)
		report.Checks["model"] = result.Message
	}
	report.Checks["api"] = fmt.Sprintf("runtime keys=%d", len(runtime.Summary()))
	graph := ExerciseProductionGraph(ctx, s, runtime)
	report.Checks["production_graph"] = fmt.Sprintf("symbols=%d", len(graph))
	return report
}
