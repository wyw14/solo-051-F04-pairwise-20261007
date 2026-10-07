package service

import (
	"context"
	"fmt"
	"github.com/wyw14/cry-161/internal/chamber"
	"github.com/wyw14/cry-161/internal/interlock"
	"github.com/wyw14/cry-161/internal/journal"
	"github.com/wyw14/cry-161/internal/model"
	"github.com/wyw14/cry-161/internal/optical"
	"github.com/wyw14/cry-161/internal/quartz"
	"github.com/wyw14/cry-161/internal/run"
	"github.com/wyw14/cry-161/internal/shutter"
	"github.com/wyw14/cry-161/internal/source"
)

func ExerciseProductionGraph(ctx context.Context, svc *Service, runtime *Runtime) map[string]any {
	result := map[string]any{}
	driver := shutter.NewDriver()
	driver.SetAvailable(true)
	_ = driver.Open()
	result["shutter_open"] = driver.Opened()
	result["source_execute"] = ExecuteError(source.ExecuteWithDriver(func() error { return nil }, driver))
	result["overcoat"] = ExecuteError(interlock.Overcoat(ctx, driver, 1, 10))
	coordinator := &run.LayerCoordinator{Source: source.NewController(10)}
	result["completed_layers"] = coordinator.Completed()
	broadcast := optical.NewBroadcaster()
	subscription, _, _ := broadcast.Subscribe(1)
	broadcast.Unsubscribe(subscription)
	result["window_values"] = len(runtime.Window.Values())
	result["monitor_samples"] = len(runtime.Monitor.Samples())
	result["monitor_estimate"] = runtime.Monitor.Estimate(10, 0.9).Nanometers
	svc.Chamber.SetShutter("closed")
	result["chamber_ready"] = svc.Chamber.Ready()
	chamber.Cool(svc.Chamber)
	alarms := interlock.NewMonitor()
	alarms.Alarm("graph-audit")
	result["alarms"] = len(alarms.Alarms())
	decision := runtime.InterlockAudit.Record(model.NewID(), true, "graph-audit")
	result["decision_valid"] = runtime.InterlockAudit.Validate(decision) == nil
	result["decisions"] = len(runtime.InterlockAudit.Decisions())
	receiver := quartz.NewReceiver()
	stop := make(chan struct{})
	go receiver.Run(stop, func(sample model.ThicknessSample) { runtime.Monitor.Add(sample) })
	receiver.Stream() <- []float64{10}
	receiver.CloseStream()
	result["quartz_stream"] = "closed"
	recipes := svc.Recipes.List()
	if len(recipes) > 0 {
		draft := svc.Drafts.Save(recipes[0])
		layer := model.Layer{Number: 1, Material: "SiO2", TargetNanometers: 100, RateNanometers: 1}
		result["draft_update"] = ExecuteError(svc.UpdateDraft(draft.ID, layer))
	}
	svc.Source.ConfigureBusy(false)
	svc.Source.ConfigurePreheatFailures(0)
	prepareErr := svc.Prepare(ctx, model.Layer{Number: 1, Material: "SiO2", TargetNanometers: 1, RateNanometers: 1})
	result["prepare"] = ExecuteError(prepareErr)
	svc.Source.Stop()
	result["admission_used"] = svc.Source.AdmissionUsed()
	result["retry_budget"] = run.RetryBudget(source.ErrSourceBusy, 0)
	result["release_quality"] = ExecuteError(run.ReleaseQuality(runtime.Monitor))
	event := journal.RunStateChanged(model.Run{ID: model.NewID(), State: model.Cooling}, model.Coating)
	result["state_event"] = event.Kind
	runs := svc.Runs.List()
	if len(runs) > 0 {
		starter := &run.Starter{Registry: svc.Runs, Vacuum: svc.Vacuum}
		result["start_coating"] = ExecuteError(starter.StartCoating(ctx, runs[0].ID))
		scheduled, err := svc.Schedule(ctx, runtime, runs[0].ID, []model.Layer{{Number: 1, Material: "SiO2", TargetNanometers: 1, RateNanometers: 1}})
		result["schedule"] = fmt.Sprintf("items=%d error=%s", len(scheduled), ExecuteError(err))
	}
	return result
}

func ExecuteError(err error) string {
	if err == nil {
		return "ok"
	}
	return err.Error()
}
