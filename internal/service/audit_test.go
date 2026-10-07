package service

import (
	"context"
	"strings"
	"testing"
)

func TestAuditOpticalReconnectCheck(t *testing.T) {
	svc := New()
	runtime := NewRuntime()
	report := svc.Audit(context.Background(), runtime)
	check, ok := report.Checks["optical_reconnect"]
	if !ok {
		t.Fatal("audit missing optical_reconnect check")
	}
	if !strings.Contains(check, "observed=true") || !strings.Contains(check, "cursor=1") {
		t.Fatalf("optical_reconnect = %q, want observed=true cursor=1", check)
	}
}
