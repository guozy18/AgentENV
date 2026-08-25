package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"google.golang.org/grpc"
)

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
		if got := r.Header.Get("X-API-Key"); got != testAPIKey {
			t.Errorf("placement API key = %q, want %s", got, testAPIKey)
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
	req.Header.Set("X-API-Key", testAPIKey)
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

	req := httptest.NewRequest(http.MethodPost, "http://gateway.test/sandboxes", strings.NewReader(`{"templateID":"snap-1"}`))
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
