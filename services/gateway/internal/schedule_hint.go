package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

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
		body, err := captureRequestBody(r)
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

// Match Axum's downstream JSON extractor limit so the gateway can inspect a
// NewSandbox request without rejecting a body the node would accept.
const maxNewSandboxBodyBytes = 2 * 1024 * 1024

var errNewSandboxBodyTooLarge = fmt.Errorf("sandbox request body exceeds %d bytes", maxNewSandboxBodyBytes)

func captureNewSandboxBody(r *http.Request) ([]byte, error) {
	if r.ContentLength > maxNewSandboxBodyBytes {
		return nil, errNewSandboxBodyTooLarge
	}
	if r.Body == nil || r.Body == http.NoBody {
		return nil, nil
	}
	orig := r.Body
	body, err := io.ReadAll(io.LimitReader(orig, maxNewSandboxBodyBytes+1))
	_ = orig.Close()
	if err != nil {
		return nil, fmt.Errorf("read sandbox request body: %w", err)
	}
	if len(body) > maxNewSandboxBodyBytes {
		return nil, errNewSandboxBodyTooLarge
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	return body, nil
}

// captureRequestBody buffers up to maxHintBodyBytes of the request body so a
// scheduling hint can be extracted, then restores r.Body so the full body
// remains available for the upstream request. If the body exceeds the budget,
// the buffered prefix is stitched back in front of the unread remainder (no
// full buffering) and a nil body is returned so hint extraction is skipped.
func captureRequestBody(r *http.Request) ([]byte, error) {
	if r.Body == nil || r.Body == http.NoBody {
		return nil, nil
	}
	orig := r.Body
	buf, err := io.ReadAll(io.LimitReader(orig, maxHintBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(buf)) > maxHintBodyBytes {
		// Too large to inspect: restore the full stream without buffering the
		// remainder and skip hint extraction.
		r.Body = &prefixedBody{Reader: io.MultiReader(bytes.NewReader(buf), orig), closer: orig}
		return nil, nil
	}
	_ = orig.Close()
	r.Body = io.NopCloser(bytes.NewReader(buf))
	r.ContentLength = int64(len(buf))
	return buf, nil
}

// prefixedBody re-presents an already-partially-read body as a single
// ReadCloser: the buffered prefix followed by the unread remainder, while
// closing the underlying body.
type prefixedBody struct {
	io.Reader
	closer io.Closer
}

func (b *prefixedBody) Close() error { return b.closer.Close() }

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
