package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/wyw14/cry-161/internal/journal"
	"github.com/wyw14/cry-161/internal/optical"
	"github.com/wyw14/cry-161/internal/service"
)

const (
	defaultReplayLimit = 256
	maxReplayLimit     = 2048
	sseKeepAlive       = 30 * time.Second
)

func parseCursor(raw string) (optical.Cursor, bool) {
	if raw == "" {
		return optical.Cursor{}, true
	}
	seq, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || seq < 0 {
		return optical.Cursor{}, false
	}
	return optical.Cursor{Seq: seq}, true
}

func parseLimit(raw string) int {
	if raw == "" {
		return defaultReplayLimit
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 1 {
		return defaultReplayLimit
	}
	if limit > maxReplayLimit {
		return maxReplayLimit
	}
	return limit
}

// thickness serves a one-shot trace replay: samples strictly after the
// cursor, oldest first. It reads the trace only, so it never feeds the
// safety Monitor.
func thickness(svc *service.Service, runtime *service.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_ = svc
		cursor, ok := parseCursor(r.URL.Query().Get("cursor"))
		if !ok {
			http.Error(w, "invalid cursor", http.StatusBadRequest)
			return
		}
		limit := parseLimit(r.URL.Query().Get("limit"))
		report, err := runtime.Replay(r.Context(), cursor, limit)
		if err != nil {
			if errors.Is(err, journal.ErrCursorExpired) {
				http.Error(w, err.Error(), http.StatusGone)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, report)
	}
}

// thicknessStream is the SSE subscription:
//
//	event: subscription  — initial dropped/last_seq/cursor envelope
//	event: status        — later overflow/drop updates, carrying a new cursor
//	event: sample        — live sample
//	event: replay        — gap sample recovered from trace (never re-enters
//	                       the safety monitor)
//
// Reconnect by passing Last-Event-ID or ?cursor=<seq>. The live path never
// blocks safety control: publisher-side drops are counted and reported.
func thicknessStream(svc *service.Service, runtime *service.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_ = svc
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		cursor, ok := parseCursor(lastEventID(r))
		if !ok {
			http.Error(w, "invalid cursor", http.StatusBadRequest)
			return
		}
		options := optical.ConsumerOptions{
			Buffer:         16,
			Events:         256,
			ReplayInterval: 50 * time.Millisecond,
		}
		// An explicit cursor (even zero) requests history; its absence
		// means pure live-from-now.
		if r.URL.Query().Has("cursor") || r.Header.Get("Last-Event-ID") != "" {
			options.From = &cursor
		}
		consumer := runtime.NewConsumer(nil, options)
		go consumer.Run(r.Context().Done())
		defer consumer.Close()

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)

		send := func(name string, id int64, value any) bool {
			data, err := json.Marshal(value)
			if err != nil {
				return false
			}
			line := ""
			if id >= 0 {
				line = fmt.Sprintf("id: %d\n", id)
			}
			if _, err := fmt.Fprintf(w, "%sevent: %s\ndata: %s\n\n", line, name, data); err != nil {
				return false
			}
			flusher.Flush()
			return true
		}
		if !send("subscription", consumer.Cursor().Seq, map[string]any{
			"status": consumer.Status(),
			"cursor": consumer.Cursor(),
		}) {
			return
		}

		keepAlive := time.NewTicker(sseKeepAlive)
		defer keepAlive.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-keepAlive.C:
				if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
					return
				}
				flusher.Flush()
			case event := <-consumer.Events():
				switch event.Kind {
				case optical.EventLive:
					if !send("sample", event.Sample.Seq, event.Sample) {
						return
					}
				case optical.EventReplay:
					if !send("replay", event.Sample.Seq, event.Sample) {
						return
					}
				case optical.EventStatus:
					if !send("status", event.Status.Cursor.Seq, event.Status) {
						return
					}
				}
			}
		}
	}
}

func lastEventID(r *http.Request) string {
	if id := r.Header.Get("Last-Event-ID"); id != "" {
		return id
	}
	return r.URL.Query().Get("cursor")
}
