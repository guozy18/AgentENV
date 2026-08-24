package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	schedulerv1 "agentenv/services/api/proto"
)

// buildScheduleHint inspects the incoming request and restores any body it
// reads. New sandbox requests also return the required snapshot reference used
// by the gateway for placement-aware routing.
func buildScheduleHint(r *http.Request) (*schedulerv1.ScheduleRequestHint, string, error) {
	if r.Method != http.MethodPost {
		return nil, "", nil
	}
	switch strings.TrimRight(r.URL.Path, "/") {
	case "/sandboxes-cold":
		body, _, err := captureRequestBody(r)
		if err != nil {
			return nil, "", err
		}
		return &schedulerv1.ScheduleRequestHint{
			Kind: &schedulerv1.ScheduleRequestHint_NewColdSandbox{
				NewColdSandbox: parseNewColdSandboxHint(body),
			},
		}, "", nil
	case "/sandboxes":
		body, err := captureNewSandboxBody(r)
		if err != nil {
			return nil, "", err
		}
		hint, snapshotRef, err := parseNewSandboxHint(body)
		if err != nil {
			if buffered, ok := r.Body.(*bufferedBody); ok {
				_ = buffered.Close()
			}
			return nil, "", err
		}
		return &schedulerv1.ScheduleRequestHint{
			Kind: &schedulerv1.ScheduleRequestHint_NewSandbox{
				NewSandbox: hint,
			},
		}, snapshotRef, nil
	default:
		return nil, "", nil
	}
}

// maxHintBodyBytes bounds how much of a request body the gateway buffers in
// memory while extracting a scheduling hint. Cold-sandbox creation bodies are
// small, so anything larger is assumed not worth inspecting. Keeping a bound
// here matters because hint extraction runs before upstream authentication;
// without it an unauthenticated client could force the gateway to buffer an
// arbitrarily large body.
const maxHintBodyBytes = 64 * 1024

// Large NewSandbox bodies must be retained until placement completes so the
// proxy can replay their original bytes. Bound both each retained body and the number
// held concurrently: this is a JSON control-plane request, not a file-upload
// endpoint, and it is inspected before upstream authentication. The per-body
// limit matches Axum's downstream Json extractor default, so the gateway does
// not reject any request that the node would otherwise accept.
const (
	maxNewSandboxBodyBytes          = 2 * 1024 * 1024
	maxConcurrentSandboxBodyBuffers = 4
)

var (
	errNewSandboxBodyTooLarge = fmt.Errorf("sandbox request body exceeds %d bytes", maxNewSandboxBodyBytes)
	errSandboxBodyBufferBusy  = errors.New("gateway sandbox request body buffer capacity is exhausted")
	sandboxBodyBufferSlots    = make(chan struct{}, maxConcurrentSandboxBodyBuffers)
)

func captureNewSandboxBody(r *http.Request) ([]byte, error) {
	if r.ContentLength > maxNewSandboxBodyBytes {
		return nil, errNewSandboxBodyTooLarge
	}
	body, oversized, err := captureRequestBody(r)
	if err != nil {
		return nil, err
	}
	if !oversized {
		return body, nil
	}
	return bufferOversizedSandboxBody(r)
}

// captureRequestBody buffers up to maxHintBodyBytes of the request body so a
// scheduling hint can be extracted, then restores r.Body so the full body
// remains available for the upstream request. If the body exceeds the budget,
// the buffered prefix is stitched back in front of the unread remainder (no
// full buffering) and oversized is returned so callers can choose a bounded
// streaming/spooling path.
func captureRequestBody(r *http.Request) ([]byte, bool, error) {
	if r.Body == nil || r.Body == http.NoBody {
		return nil, false, nil
	}
	orig := r.Body
	buf, err := io.ReadAll(io.LimitReader(orig, maxHintBodyBytes+1))
	if err != nil {
		return nil, false, err
	}
	if int64(len(buf)) > maxHintBodyBytes {
		// Too large for the in-memory inspection budget: restore the full
		// stream without buffering the remainder and let the caller choose a
		// bounded buffering path.
		r.Body = &prefixedBody{Reader: io.MultiReader(bytes.NewReader(buf), orig), closer: orig}
		return nil, true, nil
	}
	_ = orig.Close()
	r.Body = io.NopCloser(bytes.NewReader(buf))
	r.ContentLength = int64(len(buf))
	return buf, false, nil
}

// prefixedBody re-presents an already-partially-read body as a single
// ReadCloser: the buffered prefix followed by the unread remainder, while
// closing the underlying body.
type prefixedBody struct {
	io.Reader
	closer io.Closer
}

func (b *prefixedBody) Close() error { return b.closer.Close() }

// bufferedBody replays an oversized request body after its bounded inspection.
// The slot stays held until the proxy closes the body, which bounds retained
// memory even when placement or the upstream request is slow.
type bufferedBody struct {
	*bytes.Reader
	release   func()
	closeOnce sync.Once
}

func (b *bufferedBody) Close() error {
	b.closeOnce.Do(func() {
		if b.release != nil {
			b.release()
			b.release = nil
		}
	})
	return nil
}

func bufferOversizedSandboxBody(r *http.Request) ([]byte, error) {
	if err := r.Context().Err(); err != nil {
		return nil, err
	}
	select {
	case sandboxBodyBufferSlots <- struct{}{}:
	default:
		return nil, errSandboxBodyBufferBusy
	}
	release := func() { <-sandboxBodyBufferSlots }
	body, err := io.ReadAll(io.LimitReader(r.Body, maxNewSandboxBodyBytes+1))
	if err != nil {
		_ = r.Body.Close()
		release()
		return nil, fmt.Errorf("buffer sandbox request body: %w", err)
	}
	_ = r.Body.Close()
	if len(body) > maxNewSandboxBodyBytes {
		release()
		return nil, errNewSandboxBodyTooLarge
	}
	r.Body = &bufferedBody{Reader: bytes.NewReader(body), release: release}
	r.ContentLength = int64(len(body))
	return body, nil
}

// newColdSandboxBody mirrors the subset of NewColdSandbox
// (src/api/openapi.yml) that is relevant for scheduling.
type newColdSandboxBody struct {
	Image          string            `json:"image"`
	CPUCount       uint32            `json:"cpuCount"`
	MemoryMB       uint64            `json:"memoryMB"`
	Metadata       map[string]string `json:"metadata"`
	AttachedDrives []struct {
		Source struct {
			Image string `json:"image"`
		} `json:"source"`
	} `json:"attachedDrives"`
}

// parseNewColdSandboxHint extracts the structured cold-sandbox hint from the
// request body. Malformed or partial bodies yield a best-effort hint rather
// than an error, since scheduling hints are advisory.
func parseNewColdSandboxHint(body []byte) *schedulerv1.NewColdSandboxHint {
	hint := &schedulerv1.NewColdSandboxHint{}
	var parsed newColdSandboxBody
	if err := json.Unmarshal(body, &parsed); err != nil {
		return hint
	}
	hint.CpuCount = parsed.CPUCount
	hint.MemoryMb = parsed.MemoryMB
	hint.Metadata = parsed.Metadata
	if parsed.Image != "" {
		hint.Images = append(hint.Images, parsed.Image)
	}
	for _, drive := range parsed.AttachedDrives {
		if drive.Source.Image != "" {
			hint.Images = append(hint.Images, drive.Source.Image)
		}
	}
	return hint
}

// newSandboxBody mirrors the subset of NewSandbox (src/api/openapi.yml) that is
// relevant for scheduling.
type newSandboxBody struct {
	TemplateID string            `json:"templateID"`
	Metadata   map[string]string `json:"metadata"`
}

// parseNewSandboxHint extracts the required snapshot reference together with
// the advisory scheduler metadata.
func parseNewSandboxHint(body []byte) (*schedulerv1.NewSandboxHint, string, error) {
	if len(body) == 0 {
		return nil, "", errors.New("templateID is required")
	}
	var parsed newSandboxBody
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, "", fmt.Errorf("invalid sandbox request body: %w", err)
	}
	if strings.TrimSpace(parsed.TemplateID) == "" {
		return nil, "", errors.New("templateID is required")
	}
	return &schedulerv1.NewSandboxHint{Metadata: parsed.Metadata}, parsed.TemplateID, nil
}
