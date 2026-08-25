package gateway

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func sandboxRequestBodyOfSize(t *testing.T, size int) string {
	t.Helper()
	prefix := `{"templateID":"tmpl","pad":"`
	suffix := `"}`
	if size < len(prefix)+len(suffix) {
		t.Fatalf("sandbox body size %d is too small", size)
	}
	return prefix + strings.Repeat("x", size-len(prefix)-len(suffix)) + suffix
}

func newHintRequest(t *testing.T, method, target, body string) *http.Request {
	t.Helper()
	var r *http.Request
	var err error
	if body == "" {
		r, err = http.NewRequest(method, target, nil)
	} else {
		r, err = http.NewRequest(method, target, strings.NewReader(body))
	}
	if err != nil {
		t.Fatalf("build request failed: %v", err)
	}
	return r
}

func TestBuildScheduleHintNewSandbox(t *testing.T) {
	r := newHintRequest(t, http.MethodPost, "/sandboxes", `{"templateID":"tmpl"}`)

	hint, snapshotRef, err := buildScheduleHint(r)
	if err != nil {
		t.Fatalf("buildScheduleHint returned error: %v", err)
	}
	if snapshotRef != "tmpl" {
		t.Fatalf("snapshot reference = %q, want %q", snapshotRef, "tmpl")
	}
	if hint.GetNewSandbox() == nil {
		t.Fatalf("expected new_sandbox hint, got %v", hint)
	}
	if hint.GetNewColdSandbox() != nil {
		t.Fatalf("did not expect cold sandbox hint")
	}

	// Body must remain available for the upstream request.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read restored body failed: %v", err)
	}
	if string(body) != `{"templateID":"tmpl"}` {
		t.Fatalf("restored body = %q", string(body))
	}
}

func TestBuildScheduleHintNewColdSandbox(t *testing.T) {
	const reqBody = `{"image":"ubuntu:24.04","cpuCount":4,"memoryMB":2048,"attachedDrives":[{"source":{"image":"data:v1"}},{"source":{"image":"cache:v2"}}]}`
	r := newHintRequest(t, http.MethodPost, "/sandboxes-cold", reqBody)

	hint, snapshotRef, err := buildScheduleHint(r)
	if err != nil {
		t.Fatalf("buildScheduleHint returned error: %v", err)
	}
	if snapshotRef != "" {
		t.Fatalf("snapshot reference = %q, want empty", snapshotRef)
	}
	cold := hint.GetNewColdSandbox()
	if cold == nil {
		t.Fatalf("expected cold sandbox hint, got %v", hint)
	}
	if cold.GetCpuCount() != 4 {
		t.Fatalf("cpu_count = %d, want 4", cold.GetCpuCount())
	}
	if cold.GetMemoryMb() != 2048 {
		t.Fatalf("memory_mb = %d, want 2048", cold.GetMemoryMb())
	}
	wantImages := []string{"ubuntu:24.04", "data:v1", "cache:v2"}
	if got := cold.GetImages(); !equalStrings(got, wantImages) {
		t.Fatalf("images = %v, want %v", got, wantImages)
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read restored body failed: %v", err)
	}
	if string(body) != reqBody {
		t.Fatalf("restored body = %q", string(body))
	}
	if r.ContentLength != int64(len(reqBody)) {
		t.Fatalf("content length = %d, want %d", r.ContentLength, len(reqBody))
	}
}

func TestBuildScheduleHintTrailingSlash(t *testing.T) {

	hint, snapshotRef, err := buildScheduleHint(newHintRequest(t, http.MethodPost, "/sandboxes/", `{"templateID":"tmpl"}`))
	if err != nil {
		t.Fatalf("buildScheduleHint returned error: %v", err)
	}
	if hint.GetNewSandbox() == nil {
		t.Fatalf("expected new_sandbox hint for trailing slash, got %v", hint)
	}
	if snapshotRef != "tmpl" {
		t.Fatalf("snapshot reference = %q, want %q", snapshotRef, "tmpl")
	}

	hint, snapshotRef, err = buildScheduleHint(newHintRequest(t, http.MethodPost, "/sandboxes-cold/", `{"image":"img"}`))
	if err != nil {
		t.Fatalf("buildScheduleHint returned error: %v", err)
	}
	if hint.GetNewColdSandbox() == nil {
		t.Fatalf("expected cold sandbox hint for trailing slash, got %v", hint)
	}
}

func TestBuildScheduleHintNoHint(t *testing.T) {

	cases := []struct {
		name   string
		method string
		target string
	}{
		{"get sandboxes", http.MethodGet, "/sandboxes"},
		{"sandbox detail", http.MethodPost, "/sandboxes/sbx-1/pause"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hint, snapshotRef, err := buildScheduleHint(newHintRequest(t, tc.method, tc.target, ""))
			if err != nil {
				t.Fatalf("buildScheduleHint returned error: %v", err)
			}
			if hint != nil {
				t.Fatalf("expected nil hint, got %v", hint)
			}
			if snapshotRef != "" {
				t.Fatalf("snapshot reference = %q, want empty", snapshotRef)
			}
		})
	}
}

func TestBuildScheduleHintNewSandboxRejectsInvalidBody(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "missing templateID", body: `{"metadata":{"team":"alpha"}}`},
		{name: "malformed JSON", body: `{"templateID":`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := buildScheduleHint(newHintRequest(t, http.MethodPost, "/sandboxes", tc.body))
			if err == nil {
				t.Fatal("expected invalid sandbox body error")
			}
		})
	}
}

func TestBuildScheduleHintOversizedBodyAllowsReferenceBeyondInspectionBudget(t *testing.T) {
	templateID := strings.Repeat("t", maxHintBodyBytes+1)
	reqBody := `{"templateID":"` + templateID + `","pad":"` + strings.Repeat("a", maxHintBodyBytes) + `"}`
	if len(reqBody) >= maxNewSandboxBodyBytes {
		t.Fatalf("test body unexpectedly exceeds request limit: %d", len(reqBody))
	}

	r := newHintRequest(t, http.MethodPost, "/sandboxes", reqBody)
	hint, snapshotRef, err := buildScheduleHint(r)
	if err != nil {
		t.Fatalf("buildScheduleHint returned error: %v", err)
	}
	defer r.Body.Close()
	if hint.GetNewSandbox() == nil {
		t.Fatalf("expected new_sandbox hint, got %v", hint)
	}
	if snapshotRef != templateID {
		t.Fatalf("snapshot reference length = %d, want %d", len(snapshotRef), len(templateID))
	}
}

func TestParseNewColdSandboxHint(t *testing.T) {
	t.Run("empty body", func(t *testing.T) {
		hint := parseNewColdSandboxHint(nil)
		if hint == nil {
			t.Fatalf("expected non-nil hint")
		}
		if hint.GetCpuCount() != 0 || hint.GetMemoryMb() != 0 || len(hint.GetImages()) != 0 {
			t.Fatalf("expected zero-value hint, got %v", hint)
		}
	})

	t.Run("rootfs only", func(t *testing.T) {
		hint := parseNewColdSandboxHint([]byte(`{"image":"ubuntu:24.04"}`))
		if got := hint.GetImages(); !equalStrings(got, []string{"ubuntu:24.04"}) {
			t.Fatalf("images = %v", got)
		}
	})

	t.Run("skips empty drive images", func(t *testing.T) {
		hint := parseNewColdSandboxHint([]byte(`{"image":"img","attachedDrives":[{"source":{"image":""}},{"source":{"image":"data:v1"}}]}`))
		if got := hint.GetImages(); !equalStrings(got, []string{"img", "data:v1"}) {
			t.Fatalf("images = %v", got)
		}
	})
}

func TestBuildScheduleHintColdSandboxOversizedBodyStreams(t *testing.T) {
	// A body larger than the inspection budget must not be fully buffered:
	// hint extraction is skipped, but the full body must still reach upstream.
	prefix := `{"image":"ubuntu:24.04","pad":"`
	reqBody := prefix + strings.Repeat("a", maxHintBodyBytes) + `"}`
	if int64(len(reqBody)) <= maxHintBodyBytes {
		t.Fatalf("test body must exceed the budget")
	}
	r := newHintRequest(t, http.MethodPost, "/sandboxes-cold", reqBody)

	hint, snapshotRef, err := buildScheduleHint(r)
	if err != nil {
		t.Fatalf("buildScheduleHint returned error: %v", err)
	}
	if snapshotRef != "" {
		t.Fatalf("snapshot reference = %q, want empty", snapshotRef)
	}
	cold := hint.GetNewColdSandbox()
	if cold == nil {
		t.Fatalf("expected cold sandbox hint, got %v", hint)
	}
	// Oversized body is not inspected, so no fields are extracted.
	if len(cold.GetImages()) != 0 || cold.GetCpuCount() != 0 || cold.GetMemoryMb() != 0 {
		t.Fatalf("expected empty hint for oversized body, got %v", cold)
	}

	// The full original body must still be readable for the upstream request.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read restored body failed: %v", err)
	}
	if string(body) != reqBody {
		t.Fatalf("restored body length = %d, want %d", len(body), len(reqBody))
	}
}
