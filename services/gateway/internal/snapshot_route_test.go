package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSnapshotRequestOperation(t *testing.T) {
	tests := []struct {
		name              string
		method            string
		target            string
		launchSnapshotRef string
		wantOperation     snapshotOperation
		wantRef           string
	}{
		{
			name:              "sandbox launch",
			method:            http.MethodPost,
			target:            "/sandboxes",
			launchSnapshotRef: "team/base:v1",
			wantOperation:     snapshotOperationLaunch,
			wantRef:           "team/base:v1",
		},
		{
			name:          "snapshot promotion",
			method:        http.MethodPost,
			target:        "/snapshots/snap-1/promote",
			wantOperation: snapshotOperationPromote,
			wantRef:       "snap-1",
		},
		{
			name:          "escaped promotion reference",
			method:        http.MethodPost,
			target:        "/snapshots/team%2Fsnap%3Av1/promote",
			wantOperation: snapshotOperationPromote,
			wantRef:       "team/snap:v1",
		},
		{name: "snapshot get", method: http.MethodGet, target: "/snapshots/snap-1", wantOperation: snapshotOperationNone},
		{name: "snapshot capture", method: http.MethodPost, target: "/sandboxes/sbx-1/snapshots", wantOperation: snapshotOperationNone},
		{name: "nested promotion path", method: http.MethodPost, target: "/snapshots/a/b/promote", wantOperation: snapshotOperationNone},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, "http://gateway.test"+tc.target, nil)
			operation, snapshotRef := snapshotRequestOperation(req, tc.launchSnapshotRef)
			if operation != tc.wantOperation || snapshotRef != tc.wantRef {
				t.Fatalf("operation = (%v, %q), want (%v, %q)", operation, snapshotRef, tc.wantOperation, tc.wantRef)
			}
		})
	}
}

func TestLookupSnapshotPlacementUsesOpaqueQueryAndSelectedHeaders(t *testing.T) {
	type observedRequest struct {
		snapshotRef string
		headers     http.Header
	}
	observed := make(chan observedRequest, 1)
	metadataNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observed <- observedRequest{snapshotRef: r.URL.Query().Get("snapshotID"), headers: r.Header.Clone()}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"snapshotType":"distributed"}`))
	}))
	defer metadataNode.Close()

	server := newTestServer(t, stubSchedulerClient{}, time.Second, 1024)
	incoming := httptest.NewRequest(http.MethodPost, "http://gateway.test/sandboxes", nil)
	incoming.Header.Set("Authorization", "Bearer token")
	incoming.Header.Set("X-API-Key", "api-key")
	incoming.Header.Set("X-Team-ID", "team-1")
	incoming.Header.Set("X-Admin-Token", "admin-token")
	incoming.Header.Set("Traceparent", "00-trace")
	incoming.Header.Set("Tracestate", "state")
	incoming.Header.Set("Baggage", "key=value")
	incoming.Header.Set("X-Request-ID", "request-1")
	incoming.Header.Set(headerSandboxID, "must-not-leak")
	incoming.Header.Set(headerTargetPort, "49983")
	incoming.Header.Set("Content-Length", "123")

	placement, err := server.lookupSnapshotPlacement(
		context.Background(),
		incoming,
		&schedulerv1.Node{Endpoint: metadataNode.URL},
		"team/snap:v1 & next",
	)
	if err != nil {
		t.Fatalf("lookup snapshot placement failed: %v", err)
	}
	if placement.SnapshotType != "distributed" {
		t.Fatalf("snapshot type = %q, want distributed", placement.SnapshotType)
	}

	request := <-observed
	if request.snapshotRef != "team/snap:v1 & next" {
		t.Fatalf("snapshot reference = %q", request.snapshotRef)
	}
	for name, want := range map[string]string{
		"X-API-Key":    "api-key",
		"Traceparent":  "00-trace",
		"Tracestate":   "state",
		"Baggage":      "key=value",
		"X-Request-ID": "request-1",
	} {
		if got := request.headers.Get(name); got != want {
			t.Fatalf("%s = %q, want %q", name, got, want)
		}
	}
	for _, name := range []string{
		"Authorization",
		"X-Team-ID",
		"X-Admin-Token",
		headerSandboxID,
		headerTargetPort,
		"Content-Length",
	} {
		if got := request.headers.Get(name); got != "" {
			t.Fatalf("internal placement request leaked %s = %q", name, got)
		}
	}
}

func TestRouteSnapshotRequestDistributedKeepsScheduledNode(t *testing.T) {
	metadataNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"snapshotType":"distributed"}`))
	}))
	defer metadataNode.Close()

	scheduled := &schedulerv1.Node{NodeId: "node-a", Endpoint: metadataNode.URL}
	server := newTestServer(t, stubSchedulerClient{}, time.Second, 1024)
	routed, ownerBound, routingErr := server.routeSnapshotRequest(
		context.Background(),
		httptest.NewRequest(http.MethodPost, "/sandboxes", nil),
		scheduled,
		snapshotOperationLaunch,
		"snap-1",
	)
	if routingErr != nil {
		t.Fatalf("route snapshot request failed: %v", routingErr)
	}
	if routed != scheduled {
		t.Fatal("distributed snapshot must keep the scheduled node")
	}
	if ownerBound {
		t.Fatal("distributed snapshot must not be owner-bound")
	}
}

func TestRouteSnapshotRequestLocalUsesReadyOwner(t *testing.T) {
	metadataNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"snapshotType":"local","ownerNodeID":"node-b"}`))
	}))
	defer metadataNode.Close()

	server := newTestServer(t, stubSchedulerClient{
		getNodeFunc: func(_ context.Context, req *schedulerv1.GetNodeRequest, _ ...grpc.CallOption) (*schedulerv1.GetNodeResponse, error) {
			if req.GetNodeId() != "node-b" {
				t.Fatalf("owner node id = %q, want node-b", req.GetNodeId())
			}
			return &schedulerv1.GetNodeResponse{Node: &schedulerv1.ObservedNode{
				NodeId:   "node-b",
				Endpoint: "http://node-b.test",
				Snapshot: &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
			}}, nil
		},
	}, time.Second, 1024)

	routed, ownerBound, routingErr := server.routeSnapshotRequest(
		context.Background(),
		httptest.NewRequest(http.MethodPost, "/sandboxes", nil),
		&schedulerv1.Node{NodeId: "node-a", Endpoint: metadataNode.URL},
		snapshotOperationLaunch,
		"snap-1",
	)
	if routingErr != nil {
		t.Fatalf("route snapshot request failed: %v", routingErr)
	}
	if !ownerBound {
		t.Fatal("local snapshot must be owner-bound")
	}
	if routed.GetNodeId() != "node-b" || routed.GetEndpoint() != "http://node-b.test" {
		t.Fatalf("routed node = %v, want node-b", routed)
	}
}

func TestRouteSnapshotRequestRejectsUnavailableOwner(t *testing.T) {
	tests := []struct {
		name     string
		response *schedulerv1.GetNodeResponse
		err      error
	}{
		{name: "not found", err: status.Error(codes.NotFound, "observed node not found")},
		{name: "missing node", response: &schedulerv1.GetNodeResponse{}},
		{name: "wrong node", response: observedNodeResponse("node-c", "http://node-c.test", schedulerv1.NodeStatus_NODE_STATUS_READY)},
		{name: "connecting", response: observedNodeResponse("node-b", "http://node-b.test", schedulerv1.NodeStatus_NODE_STATUS_CONNECTING)},
		{name: "unhealthy", response: observedNodeResponse("node-b", "http://node-b.test", schedulerv1.NodeStatus_NODE_STATUS_UNHEALTHY)},
		{name: "lingering", response: observedNodeResponse("node-b", "http://node-b.test", schedulerv1.NodeStatus_NODE_STATUS_LINGERING)},
		{name: "unspecified", response: observedNodeResponse("node-b", "http://node-b.test", schedulerv1.NodeStatus_NODE_STATUS_UNSPECIFIED)},
		{name: "invalid endpoint", response: observedNodeResponse("node-b", "ftp://node-b.test", schedulerv1.NodeStatus_NODE_STATUS_READY)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			metadataNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(`{"snapshotType":"local","ownerNodeID":"node-b"}`))
			}))
			defer metadataNode.Close()

			server := newTestServer(t, stubSchedulerClient{
				getNodeFunc: func(context.Context, *schedulerv1.GetNodeRequest, ...grpc.CallOption) (*schedulerv1.GetNodeResponse, error) {
					return tc.response, tc.err
				},
			}, time.Second, 1024)

			_, ownerBound, routingErr := server.routeSnapshotRequest(
				context.Background(),
				httptest.NewRequest(http.MethodPost, "/sandboxes", nil),
				&schedulerv1.Node{NodeId: "node-a", Endpoint: metadataNode.URL},
				snapshotOperationLaunch,
				"snap-1",
			)
			if routingErr == nil || routingErr.statusCode != http.StatusServiceUnavailable {
				t.Fatalf("routing error = %v, want 503", routingErr)
			}
			if ownerBound {
				t.Fatal("failed owner resolution must not return an owner-bound route")
			}
		})
	}
}

func TestRouteSnapshotRequestRejectsInvalidPlacement(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		body       string
	}{
		{name: "backend unavailable", statusCode: http.StatusInternalServerError},
		{name: "unexpected success status", statusCode: http.StatusCreated, body: `{"snapshotType":"distributed"}`},
		{name: "malformed JSON", statusCode: http.StatusOK, body: `{"snapshotType":`},
		{name: "unknown type", statusCode: http.StatusOK, body: `{"snapshotType":"temporal"}`},
		{name: "local missing owner", statusCode: http.StatusOK, body: `{"snapshotType":"local"}`},
		{name: "distributed with owner", statusCode: http.StatusOK, body: `{"snapshotType":"distributed","ownerNodeID":"node-b"}`},
		{name: "oversized response", statusCode: http.StatusOK, body: strings.Repeat("x", maxSnapshotPlacementResponseBytes+1)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			metadataNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				statusCode := tc.statusCode
				if statusCode == 0 {
					statusCode = http.StatusOK
				}
				w.WriteHeader(statusCode)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer metadataNode.Close()

			server := newTestServer(t, stubSchedulerClient{}, time.Second, 1024)
			_, _, routingErr := server.routeSnapshotRequest(
				context.Background(),
				httptest.NewRequest(http.MethodPost, "/sandboxes", nil),
				&schedulerv1.Node{NodeId: "node-a", Endpoint: metadataNode.URL},
				snapshotOperationLaunch,
				"snap-1",
			)
			if routingErr == nil || routingErr.statusCode != http.StatusServiceUnavailable {
				t.Fatalf("routing error = %v, want 503", routingErr)
			}
		})
	}
}

func TestSnapshotPlacementErrorContract(t *testing.T) {
	for _, tc := range []struct {
		name            string
		placementStatus int
		target          string
		body            string
		wantStatus      int
		wantMessage     string
		wantEmptyBody   bool
	}{
		{
			name:            "launch/invalid reference",
			placementStatus: http.StatusBadRequest,
			target:          "/sandboxes",
			body:            `{"templateID":"bad alias"}`,
			wantStatus:      http.StatusBadRequest,
			wantMessage:     "invalid snapshot reference",
		},
		{
			name:            "promote/invalid reference",
			placementStatus: http.StatusBadRequest,
			target:          "/snapshots/bad%20alias/promote",
			wantStatus:      http.StatusConflict,
			wantMessage:     "invalid snapshot reference",
		},
		{
			name:            "launch/not found",
			placementStatus: http.StatusNotFound,
			target:          "/sandboxes",
			body:            `{"templateID":"snap-1"}`,
			wantStatus:      http.StatusBadRequest,
			wantMessage:     "template snap-1 not found",
		},
		{
			name:            "promote/not found",
			placementStatus: http.StatusNotFound,
			target:          "/snapshots/snap-1/promote",
			wantStatus:      http.StatusNotFound,
			wantMessage:     "snapshot snap-1 not found",
		},
		{
			name:            "launch/unauthorized",
			placementStatus: http.StatusUnauthorized,
			target:          "/sandboxes",
			body:            `{"templateID":"snap-1"}`,
			wantStatus:      http.StatusUnauthorized,
			wantEmptyBody:   true,
		},
		{
			name:            "promote/unauthorized",
			placementStatus: http.StatusUnauthorized,
			target:          "/snapshots/snap-1/promote",
			wantStatus:      http.StatusUnauthorized,
			wantEmptyBody:   true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			originalRequest := make(chan struct{}, 1)
			metadataNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == snapshotPlacementPath {
					w.WriteHeader(tc.placementStatus)
					return
				}
				originalRequest <- struct{}{}
				w.WriteHeader(http.StatusNoContent)
			}))
			defer metadataNode.Close()

			server := newTestServer(t, stubSchedulerClient{
				scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
					return &schedulerv1.ScheduleResponse{Node: &schedulerv1.Node{NodeId: "node-a", Endpoint: metadataNode.URL}}, nil
				},
			}, time.Second, 1024)

			req := httptest.NewRequest(http.MethodPost, "http://gateway.test"+tc.target, strings.NewReader(tc.body))
			resp := httptest.NewRecorder()
			authenticatedTestHandler(server).ServeHTTP(resp, req)

			if resp.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d; body=%q", resp.Code, tc.wantStatus, resp.Body.String())
			}
			if tc.wantEmptyBody {
				if resp.Body.Len() != 0 {
					t.Fatalf("response body = %q, want empty generated-API response", resp.Body.String())
				}
			} else {
				if got := resp.Header().Get("Content-Type"); got != "application/json" {
					t.Fatalf("Content-Type = %q, want application/json", got)
				}
				var envelope struct {
					Code    int    `json:"code"`
					Message string `json:"message"`
				}
				if err := json.Unmarshal(resp.Body.Bytes(), &envelope); err != nil {
					t.Fatalf("decode error envelope: %v; body=%q", err, resp.Body.String())
				}
				if envelope.Code != tc.wantStatus || envelope.Message != tc.wantMessage {
					t.Fatalf("error envelope = %+v, want code=%d message=%q", envelope, tc.wantStatus, tc.wantMessage)
				}
			}
			select {
			case <-originalRequest:
				t.Fatal("placement error fell back to the scheduled node or reached the public upstream route")
			default:
			}
		})
	}
}

func TestLocalSnapshotLaunchRoutesToOwnerAndRecordsAssignment(t *testing.T) {
	metadataNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != snapshotPlacementPath {
			t.Errorf("metadata node received unexpected path %q", r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if got := r.URL.Query().Get("snapshotID"); got != "team/base:v1" {
			t.Errorf("placement snapshotID = %q, want %q", got, "team/base:v1")
		}
		_, _ = w.Write([]byte(`{"snapshotType":"local","ownerNodeID":"node-b"}`))
	}))
	defer metadataNode.Close()

	ownerRequests := make(chan string, 1)
	ownerNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read owner request body: %v", err)
		}
		ownerRequests <- string(body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"sandboxID":"sbx-created"}`))
	}))
	defer ownerNode.Close()

	recorded := make(chan *schedulerv1.RecordAssignmentRequest, 1)
	server := newTestServer(t, stubSchedulerClient{
		scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			return &schedulerv1.ScheduleResponse{Node: &schedulerv1.Node{NodeId: "node-a", Endpoint: metadataNode.URL}}, nil
		},
		getNodeFunc: func(_ context.Context, req *schedulerv1.GetNodeRequest, _ ...grpc.CallOption) (*schedulerv1.GetNodeResponse, error) {
			if req.GetNodeId() != "node-b" {
				t.Fatalf("GetNode owner = %q, want node-b", req.GetNodeId())
			}
			return observedNodeResponse("node-b", ownerNode.URL, schedulerv1.NodeStatus_NODE_STATUS_READY), nil
		},
		recordAssignmentFunc: func(_ context.Context, req *schedulerv1.RecordAssignmentRequest, _ ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
			recorded <- req
			return &schedulerv1.RecordAssignmentResponse{}, nil
		},
	}, time.Second, 1024, withQueryOnlyScheduler(stubSchedulerClient{
		getNodeFunc: func(context.Context, *schedulerv1.GetNodeRequest, ...grpc.CallOption) (*schedulerv1.GetNodeResponse, error) {
			t.Fatal("query-only scheduler must not resolve snapshot owners")
			return nil, nil
		},
	}))

	requestBody := `{"templateID":"team/base:v1","metadata":{"team":"alpha"}}`
	req := httptest.NewRequest(http.MethodPost, "http://gateway.test/sandboxes", strings.NewReader(requestBody))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(resp, req)

	if resp.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%q", resp.Code, resp.Body.String())
	}
	if got := <-ownerRequests; got != requestBody {
		t.Fatalf("owner request body = %q, want %q", got, requestBody)
	}
	record := <-recorded
	if record.GetSandboxId() != "sbx-created" {
		t.Fatalf("recorded sandbox = %q, want sbx-created", record.GetSandboxId())
	}
	if record.GetNode().GetNodeId() != "node-b" || record.GetNode().GetEndpoint() != ownerNode.URL {
		t.Fatalf("recorded node = %v, want owner node-b", record.GetNode())
	}
}

func TestLocalSnapshotPromotionRoutesToOwner(t *testing.T) {
	metadataNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != snapshotPlacementPath {
			t.Errorf("metadata node received unexpected path %q", r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`{"snapshotType":"local","ownerNodeID":"node-b"}`))
	}))
	defer metadataNode.Close()

	ownerPath := make(chan string, 1)
	ownerNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ownerPath <- r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ownerNode.Close()

	server := newTestServer(t, stubSchedulerClient{
		scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			return &schedulerv1.ScheduleResponse{Node: &schedulerv1.Node{NodeId: "node-a", Endpoint: metadataNode.URL}}, nil
		},
		getNodeFunc: func(context.Context, *schedulerv1.GetNodeRequest, ...grpc.CallOption) (*schedulerv1.GetNodeResponse, error) {
			return observedNodeResponse("node-b", ownerNode.URL, schedulerv1.NodeStatus_NODE_STATUS_READY), nil
		},
	}, time.Second, 1024)

	req := httptest.NewRequest(http.MethodPost, "http://gateway.test/snapshots/snap-1/promote", nil)
	resp := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(resp, req)

	if resp.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%q", resp.Code, resp.Body.String())
	}
	if got := <-ownerPath; got != "/snapshots/snap-1/promote" {
		t.Fatalf("owner path = %q", got)
	}
}

func TestDistributedSnapshotPromotionKeepsScheduledNode(t *testing.T) {
	originalRequest := make(chan string, 1)
	metadataNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == snapshotPlacementPath {
			_, _ = w.Write([]byte(`{"snapshotType":"distributed"}`))
			return
		}
		originalRequest <- r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	}))
	defer metadataNode.Close()

	server := newTestServer(t, stubSchedulerClient{
		scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			return &schedulerv1.ScheduleResponse{Node: &schedulerv1.Node{NodeId: "node-a", Endpoint: metadataNode.URL}}, nil
		},
	}, time.Second, 1024)

	req := httptest.NewRequest(http.MethodPost, "http://gateway.test/snapshots/snap-1/promote", nil)
	resp := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(resp, req)

	if resp.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%q", resp.Code, resp.Body.String())
	}
	if got := <-originalRequest; got != "/snapshots/snap-1/promote" {
		t.Fatalf("scheduled node path = %q", got)
	}
}

func TestSnapshotMetadataRequestsDoNotUsePlacementLookup(t *testing.T) {
	for _, path := range []string{"/snapshots", "/snapshots/snap-1"} {
		t.Run(path, func(t *testing.T) {
			upstreamPath := make(chan string, 1)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upstreamPath <- r.URL.Path
				w.WriteHeader(http.StatusOK)
			}))
			defer upstream.Close()

			server := newTestServer(t, stubSchedulerClient{
				scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
					return &schedulerv1.ScheduleResponse{Node: &schedulerv1.Node{NodeId: "node-a", Endpoint: upstream.URL}}, nil
				},
			}, time.Second, 1024)

			req := httptest.NewRequest(http.MethodGet, "http://gateway.test"+path, nil)
			resp := httptest.NewRecorder()
			authenticatedTestHandler(server).ServeHTTP(resp, req)

			if resp.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", resp.Code)
			}
			if got := <-upstreamPath; got != path {
				t.Fatalf("upstream path = %q, want %q", got, path)
			}
		})
	}
}

func TestSnapshotCaptureUsesSandboxLookupWithoutPlacement(t *testing.T) {
	upstreamPath := make(chan string, 1)
	owner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamPath <- r.URL.Path
		w.WriteHeader(http.StatusCreated)
	}))
	defer owner.Close()

	mainScheduler := stubSchedulerClient{
		scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			t.Fatal("snapshot capture must not call Schedule")
			return nil, nil
		},
	}
	queryOnlyScheduler := stubSchedulerClient{
		lookupNodeFunc: func(_ context.Context, req *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			if req.GetSandboxId() != "sbx-1" {
				t.Fatalf("LookupNode sandbox = %q, want sbx-1", req.GetSandboxId())
			}
			return &schedulerv1.LookupNodeResponse{Node: &schedulerv1.Node{NodeId: "node-a", Endpoint: owner.URL}}, nil
		},
	}
	server := newTestServer(t, mainScheduler, time.Second, 1024, withQueryOnlyScheduler(queryOnlyScheduler))

	req := httptest.NewRequest(
		http.MethodPost,
		"http://gateway.test/sandboxes/sbx-1/snapshots",
		strings.NewReader(`{"snapshotType":"local"}`),
	)
	resp := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(resp, req)

	if resp.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%q", resp.Code, resp.Body.String())
	}
	if got := <-upstreamPath; got != "/sandboxes/sbx-1/snapshots" {
		t.Fatalf("upstream path = %q", got)
	}
}

func TestNewSandboxRoutingInputErrorsBeforeSchedule(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{name: "missing templateID", body: `{"metadata":{"team":"alpha"}}`, wantStatus: http.StatusBadRequest},
		{name: "malformed JSON", body: `{"templateID":`, wantStatus: http.StatusBadRequest},
		{name: "empty body", body: "", wantStatus: http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scheduled := make(chan struct{}, 1)
			server := newTestServer(t, stubSchedulerClient{
				scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
					scheduled <- struct{}{}
					return nil, errors.New("Schedule must not be called")
				},
			}, time.Second, 1024)

			req := httptest.NewRequest(http.MethodPost, "http://gateway.test/sandboxes", strings.NewReader(tc.body))
			resp := httptest.NewRecorder()
			authenticatedTestHandler(server).ServeHTTP(resp, req)

			if resp.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", resp.Code, tc.wantStatus)
			}
			select {
			case <-scheduled:
				t.Fatal("invalid request reached Scheduler.Schedule")
			default:
			}
		})
	}
}

func TestOversizedNewSandboxBodyIsForwardedAndLocalOwnerRouted(t *testing.T) {
	requestBody := `{"metadata":{"pad":"` + strings.Repeat("a", maxHintBodyBytes) + `"},"templateID":"team/base:v1"}`
	ownerRequests := make(chan string, 1)

	metadataNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != snapshotPlacementPath {
			t.Errorf("scheduled node received public request path %q", r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if got := r.URL.Query().Get("snapshotID"); got != "team/base:v1" {
			t.Errorf("placement snapshotID = %q, want team/base:v1", got)
		}
		_, _ = w.Write([]byte(`{"snapshotType":"local","ownerNodeID":"node-b"}`))
	}))
	defer metadataNode.Close()

	ownerNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read owner request body: %v", err)
			return
		}
		ownerRequests <- string(body)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"sandboxID":"sbx-created"}`))
	}))
	defer ownerNode.Close()

	server := newTestServer(t, stubSchedulerClient{
		scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			return &schedulerv1.ScheduleResponse{Node: &schedulerv1.Node{NodeId: "node-a", Endpoint: metadataNode.URL}}, nil
		},
		getNodeFunc: func(_ context.Context, req *schedulerv1.GetNodeRequest, _ ...grpc.CallOption) (*schedulerv1.GetNodeResponse, error) {
			if req.GetNodeId() != "node-b" {
				t.Fatalf("GetNode owner = %q, want node-b", req.GetNodeId())
			}
			return observedNodeResponse("node-b", ownerNode.URL, schedulerv1.NodeStatus_NODE_STATUS_READY), nil
		},
		recordAssignmentFunc: func(context.Context, *schedulerv1.RecordAssignmentRequest, ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
			return &schedulerv1.RecordAssignmentResponse{}, nil
		},
	}, time.Second, 1024)

	req := httptest.NewRequest(http.MethodPost, "http://gateway.test/sandboxes", strings.NewReader(requestBody))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(resp, req)

	if resp.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%q", resp.Code, resp.Body.String())
	}
	select {
	case got := <-ownerRequests:
		if got != requestBody {
			t.Fatalf("owner request body length/content mismatch: got %d bytes, want %d", len(got), len(requestBody))
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for owner request")
	}
}

func TestSnapshotPlacementFailureDoesNotFallbackToScheduledNode(t *testing.T) {
	originalRequest := make(chan struct{}, 1)
	metadataNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == snapshotPlacementPath {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		originalRequest <- struct{}{}
		w.WriteHeader(http.StatusCreated)
	}))
	defer metadataNode.Close()

	server := newTestServer(t, stubSchedulerClient{
		scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			return &schedulerv1.ScheduleResponse{Node: &schedulerv1.Node{NodeId: "node-a", Endpoint: metadataNode.URL}}, nil
		},
	}, time.Second, 1024)

	requestBody := `{"templateID":"snap-1","pad":"` + strings.Repeat("x", maxHintBodyBytes) + `"}`
	req := httptest.NewRequest(http.MethodPost, "http://gateway.test/sandboxes", strings.NewReader(requestBody))
	resp := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(resp, req)

	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.Code)
	}
	if got := resp.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	var envelope struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode error envelope: %v; body=%q", err, resp.Body.String())
	}
	if envelope.Code != http.StatusServiceUnavailable || envelope.Message != "snapshot placement unavailable" {
		t.Fatalf("error envelope = %+v", envelope)
	}
	select {
	case <-originalRequest:
		t.Fatal("placement failure fell back to the scheduled node")
	default:
	}
	if got := len(sandboxBodyBufferSlots); got != 0 {
		t.Fatalf("held body buffer slots after placement failure = %d, want 0", got)
	}
}

func TestOversizedSandboxBodyBufferReleasedOnSchedulerFailure(t *testing.T) {
	server := newTestServer(t, stubSchedulerClient{
		scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			return nil, status.Error(codes.Unavailable, "scheduler unavailable")
		},
	}, time.Second, 1024)

	requestBody := `{"templateID":"snap-1","pad":"` + strings.Repeat("x", maxHintBodyBytes) + `"}`
	req := httptest.NewRequest(http.MethodPost, "http://gateway.test/sandboxes", strings.NewReader(requestBody))
	resp := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(resp, req)

	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%q", resp.Code, resp.Body.String())
	}
	if got := len(sandboxBodyBufferSlots); got != 0 {
		t.Fatalf("held body buffer slots after scheduler failure = %d, want 0", got)
	}
}

func TestNewSandboxBodyLimitReturnsRequestEntityTooLarge(t *testing.T) {
	scheduleCalled := false
	server := newTestServer(t, stubSchedulerClient{
		scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			scheduleCalled = true
			return nil, errors.New("must not schedule an oversized request")
		},
	}, time.Second, 1024)

	req := httptest.NewRequest(
		http.MethodPost,
		"http://gateway.test/sandboxes",
		strings.NewReader(sandboxRequestBodyOfSize(t, maxNewSandboxBodyBytes+1)),
	)
	req.ContentLength = -1
	req.TransferEncoding = []string{"chunked"}
	resp := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(resp, req)

	if resp.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413; body=%q", resp.Code, resp.Body.String())
	}
	if scheduleCalled {
		t.Fatal("oversized request reached Scheduler")
	}
	if got := len(sandboxBodyBufferSlots); got != 0 {
		t.Fatalf("held buffer slots after oversized request = %d, want 0", got)
	}
}

func TestNewSandboxBufferSaturationReturnsServiceUnavailable(t *testing.T) {
	for range cap(sandboxBodyBufferSlots) {
		sandboxBodyBufferSlots <- struct{}{}
	}
	defer func() {
		for range cap(sandboxBodyBufferSlots) {
			<-sandboxBodyBufferSlots
		}
	}()

	server := newTestServer(t, stubSchedulerClient{}, time.Second, 1024)
	req := httptest.NewRequest(
		http.MethodPost,
		"http://gateway.test/sandboxes",
		strings.NewReader(sandboxRequestBodyOfSize(t, maxHintBodyBytes+1)),
	)
	defer req.Body.Close()
	resp := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(resp, req)

	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%q", resp.Code, resp.Body.String())
	}
}

func TestSnapshotOwnerDialFailureReturnsServiceUnavailable(t *testing.T) {
	metadataNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"snapshotType":"local","ownerNodeID":"node-b"}`))
	}))
	defer metadataNode.Close()

	server := newTestServer(t, stubSchedulerClient{
		scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			return &schedulerv1.ScheduleResponse{Node: &schedulerv1.Node{NodeId: "node-a", Endpoint: metadataNode.URL}}, nil
		},
		getNodeFunc: func(context.Context, *schedulerv1.GetNodeRequest, ...grpc.CallOption) (*schedulerv1.GetNodeResponse, error) {
			return observedNodeResponse("node-b", "http://127.0.0.1:1", schedulerv1.NodeStatus_NODE_STATUS_READY), nil
		},
	}, time.Second, 1024)

	req := httptest.NewRequest(http.MethodPost, "http://gateway.test/sandboxes", strings.NewReader(`{"templateID":"snap-1"}`))
	resp := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(resp, req)

	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%q", resp.Code, resp.Body.String())
	}
}

func observedNodeResponse(nodeID, endpoint string, nodeStatus schedulerv1.NodeStatus) *schedulerv1.GetNodeResponse {
	return &schedulerv1.GetNodeResponse{Node: &schedulerv1.ObservedNode{
		NodeId:   nodeID,
		Endpoint: endpoint,
		Snapshot: &schedulerv1.NodeSnapshot{Status: nodeStatus},
	}}
}
