package api

import (
	"bufio"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/wyw14/cry-161/internal/model"
	"github.com/wyw14/cry-161/internal/service"
)

func validSample(runID model.ID, nm float64) model.ThicknessSample {
	return model.ThicknessSample{RunID: runID, Frequency: 10, Transmission: 0.9, Nanometers: nm, Valid: true, At: time.Now().UTC()}
}

func TestThicknessReplayEndpoint(t *testing.T) {
	runtime := service.NewRuntime()
	runID := model.NewID()
	if _, err := runtime.Capture(validSample(runID, 5), 1); err != nil {
		t.Fatal(err)
	}

	handler := thickness(nil, runtime)
	req := httptest.NewRequest(http.MethodGet, "/api/thickness?cursor=0", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"seq":1`) {
		t.Fatalf("replay body missing seq 1: %s", rec.Body.String())
	}

	// A malformed cursor is rejected.
	req = httptest.NewRequest(http.MethodGet, "/api/thickness?cursor=abc", nil)
	rec = httptest.NewRecorder()
	thickness(nil, runtime).ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad cursor status = %d", rec.Code)
	}
}

// readSSEBlock reads one blank-line-terminated SSE block.
func readSSEBlock(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	var lines []string
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("reading SSE: %v", err)
		}
		line = strings.TrimRight(line, "\n")
		if line == "" {
			return strings.Join(lines, "\n")
		}
		lines = append(lines, line)
	}
}

func TestThicknessStreamReplayThenLive(t *testing.T) {
	runtime := service.NewRuntime()
	runID := model.NewID()
	if _, err := runtime.Capture(validSample(runID, 5), 1); err != nil {
		t.Fatal(err) // seq 1 exists before the client connects
	}

	server := httptest.NewServer(thicknessStream(nil, runtime))
	defer server.Close()

	req, _ := http.NewRequest(http.MethodGet, server.URL+"/?cursor=0", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("content-type = %q", resp.Header.Get("Content-Type"))
	}
	reader := bufio.NewReader(resp.Body)

	subBlock := readSSEBlock(t, reader)
	if !strings.HasPrefix(subBlock, "id: 0\nevent: subscription") {
		t.Fatalf("first block = %q", subBlock)
	}

	replayBlock := readSSEBlock(t, reader)
	if !strings.Contains(replayBlock, "event: replay") || !strings.Contains(replayBlock, `"seq":1`) {
		t.Fatalf("expected replay of seq 1, got %q", replayBlock)
	}

	// A fresh capture after connection must come through as live.
	if _, err := runtime.Capture(validSample(runID, 6), 1); err != nil {
		t.Fatal(err)
	}
	liveBlock := readSSEBlock(t, reader)
	if !strings.HasPrefix(liveBlock, "id: 2\nevent: sample") || !strings.Contains(liveBlock, `"seq":2`) {
		t.Fatalf("expected live sample seq 2, got %q", liveBlock)
	}
}

func TestThicknessStreamLastEventIDReconnect(t *testing.T) {
	runtime := service.NewRuntime()
	runID := model.NewID()
	if _, err := runtime.Capture(validSample(runID, 5), 1); err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(thicknessStream(nil, runtime))
	defer server.Close()

	req, _ := http.NewRequest(http.MethodGet, server.URL, nil)
	req.Header.Set("Last-Event-ID", "0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	reader := bufio.NewReader(resp.Body)
	readSSEBlock(t, reader) // subscription
	replayBlock := readSSEBlock(t, reader)
	if !strings.Contains(replayBlock, "event: replay") {
		t.Fatalf("Last-Event-ID reconnect should replay gap: %q", replayBlock)
	}
}
