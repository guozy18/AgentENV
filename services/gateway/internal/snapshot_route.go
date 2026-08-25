package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	schedulerv1 "agentenv/services/api/proto"
)

const (
	snapshotPlacementPath = "/internal/snapshots/placement"
)

var (
	errSnapshotPlacementNotFound     = errors.New("snapshot placement not found")
	errSnapshotPlacementUnauthorized = errors.New("snapshot placement unauthorized")
	errSnapshotPlacementInvalid      = errors.New("snapshot placement request invalid")
)

type snapshotOperation int

const (
	snapshotOperationNone snapshotOperation = iota
	snapshotOperationLaunch
	snapshotOperationPromote
)

type snapshotPlacement struct {
	SnapshotType string `json:"snapshotType"`
	OwnerNodeID  string `json:"ownerNodeID,omitempty"`
}

func snapshotRequestOperation(r *http.Request, launchSnapshotRef string) (snapshotOperation, string) {
	if launchSnapshotRef != "" {
		return snapshotOperationLaunch, launchSnapshotRef
	}
	if r.Method != http.MethodPost {
		return snapshotOperationNone, ""
	}

	escapedPath := strings.Trim(requestEscapedPath(r), "/")
	parts := strings.Split(escapedPath, "/")
	if len(parts) != 3 || parts[1] == "" {
		return snapshotOperationNone, ""
	}
	resource, err := url.PathUnescape(parts[0])
	if err != nil || resource != "snapshots" {
		return snapshotOperationNone, ""
	}
	action, err := url.PathUnescape(parts[2])
	if err != nil || action != "promote" {
		return snapshotOperationNone, ""
	}
	snapshotRef, err := url.PathUnescape(parts[1])
	if err != nil || strings.TrimSpace(snapshotRef) == "" {
		return snapshotOperationNone, ""
	}
	return snapshotOperationPromote, snapshotRef
}

func isSnapshotPromotionRequest(r *http.Request) bool {
	operation, _ := snapshotRequestOperation(r, "")
	return operation == snapshotOperationPromote
}

// Snapshot metadata is served by the control-plane API. Header-routed proxy
// traffic is only a data-plane fallback for otherwise unmatched paths, so
// these explicit metadata routes must retain control-plane authentication and
// scheduling even when clients attach sandbox routing headers globally.
func isSnapshotMetadataRequest(r *http.Request) bool {
	if r.Method != http.MethodGet {
		return false
	}

	parts := strings.Split(strings.Trim(requestEscapedPath(r), "/"), "/")
	first, err := url.PathUnescape(parts[0])
	if err != nil || first != "snapshots" {
		return false
	}
	if len(parts) == 1 {
		return true
	}
	if len(parts) != 2 {
		return false
	}
	id, err := url.PathUnescape(parts[1])
	return err == nil && id != ""
}

func (s *Server) routeSnapshotRequest(
	ctx context.Context,
	incoming *http.Request,
	scheduledNode *schedulerv1.Node,
	operation snapshotOperation,
	snapshotRef string,
) (*schedulerv1.Node, bool, *proxyResponseError) {
	placement, err := s.lookupSnapshotPlacement(ctx, incoming, scheduledNode, snapshotRef)
	if err != nil {
		if errors.Is(err, errSnapshotPlacementUnauthorized) {
			return nil, false, &proxyResponseError{
				statusCode: http.StatusUnauthorized,
				message:    "authentication required",
			}
		}
		if errors.Is(err, errSnapshotPlacementNotFound) {
			if operation == snapshotOperationLaunch {
				return nil, false, &proxyResponseError{
					statusCode: http.StatusBadRequest,
					message:    fmt.Sprintf("template %s not found", snapshotRef),
				}
			}
			return nil, false, &proxyResponseError{
				statusCode: http.StatusNotFound,
				message:    fmt.Sprintf("snapshot %s not found", snapshotRef),
			}
		}
		if errors.Is(err, errSnapshotPlacementInvalid) {
			statusCode := http.StatusBadRequest
			if operation == snapshotOperationPromote {
				statusCode = http.StatusConflict
			}
			return nil, false, &proxyResponseError{
				statusCode: statusCode,
				message:    "invalid snapshot reference",
			}
		}
		return nil, false, &proxyResponseError{
			statusCode: http.StatusServiceUnavailable,
			message:    "snapshot placement unavailable",
			cause:      err,
		}
	}

	switch placement.SnapshotType {
	case "distributed":
		if strings.TrimSpace(placement.OwnerNodeID) != "" {
			return nil, false, invalidSnapshotPlacement("distributed snapshot must not include ownerNodeID")
		}
		return scheduledNode, false, nil
	case "local":
		ownerNodeID := strings.TrimSpace(placement.OwnerNodeID)
		if ownerNodeID == "" {
			return nil, false, invalidSnapshotPlacement("local snapshot is missing ownerNodeID")
		}
		return s.resolveReadySnapshotOwner(ctx, ownerNodeID)
	default:
		return nil, false, invalidSnapshotPlacement("snapshotType must be local or distributed")
	}
}

func invalidSnapshotPlacement(message string) *proxyResponseError {
	return &proxyResponseError{
		statusCode: http.StatusServiceUnavailable,
		message:    "snapshot placement unavailable",
		cause:      errors.New(message),
	}
}

func (s *Server) lookupSnapshotPlacement(
	ctx context.Context,
	incoming *http.Request,
	scheduledNode *schedulerv1.Node,
	snapshotRef string,
) (*snapshotPlacement, error) {
	query := url.Values{}
	query.Set("snapshotID", snapshotRef)
	target, err := joinUpstream(
		scheduledNode.GetEndpoint(),
		snapshotPlacementPath,
		snapshotPlacementPath,
		query.Encode(),
	)
	if err != nil {
		return nil, fmt.Errorf("build snapshot placement URL: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, fmt.Errorf("build snapshot placement request: %w", err)
	}
	for _, value := range incoming.Header.Values("X-API-Key") {
		req.Header.Add("X-API-Key", value)
	}
	req.Header.Set("Accept", "application/json")

	client := *s.httpClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request snapshot placement: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusUnauthorized:
		return nil, errSnapshotPlacementUnauthorized
	case http.StatusBadRequest:
		return nil, errSnapshotPlacementInvalid
	case http.StatusNotFound:
		return nil, errSnapshotPlacementNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("snapshot placement returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read snapshot placement response: %w", err)
	}
	var placement snapshotPlacement
	if err := json.Unmarshal(body, &placement); err != nil {
		return nil, fmt.Errorf("decode snapshot placement response: %w", err)
	}
	return &placement, nil
}

func (s *Server) resolveReadySnapshotOwner(
	ctx context.Context,
	ownerNodeID string,
) (*schedulerv1.Node, bool, *proxyResponseError) {
	rpcStart := time.Now()
	resp, err := s.scheduler.GetNode(ctx, &schedulerv1.GetNodeRequest{NodeId: ownerNodeID})
	recordGatewaySchedulerRPC("GetNode", rpcStart, err)
	if err != nil {
		return nil, false, &proxyResponseError{
			statusCode: http.StatusServiceUnavailable,
			message:    "snapshot owner unavailable",
			cause:      err,
		}
	}

	observed := resp.GetNode()
	if observed.GetNodeId() != ownerNodeID {
		return nil, false, unavailableSnapshotOwner("scheduler returned a different snapshot owner")
	}
	if observed.GetSnapshot().GetStatus() != schedulerv1.NodeStatus_NODE_STATUS_READY {
		return nil, false, unavailableSnapshotOwner("snapshot owner is not ready")
	}
	parsedEndpoint, parseErr := url.Parse(strings.TrimSpace(observed.GetEndpoint()))
	if parseErr != nil ||
		(parsedEndpoint.Scheme != "http" && parsedEndpoint.Scheme != "https") ||
		parsedEndpoint.Host == "" {
		return nil, false, unavailableSnapshotOwner("snapshot owner endpoint is invalid")
	}

	return &schedulerv1.Node{
		NodeId:   observed.GetNodeId(),
		Endpoint: observed.GetEndpoint(),
	}, true, nil
}

func unavailableSnapshotOwner(message string) *proxyResponseError {
	return &proxyResponseError{
		statusCode: http.StatusServiceUnavailable,
		message:    "snapshot owner unavailable",
		cause:      errors.New(message),
	}
}
