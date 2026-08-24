package scheduler

import (
	"testing"
	"time"

	"agentenv/services/shared/config"

	schedulerv1 "agentenv/services/api/proto"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
)

var defaultDiscoveryCfg = config.SchedulerDiscoveryKubernetesConfig{
	Namespace:   "agentenv-system",
	ServiceName: "agentenv-nodes",
	Port:        8000,
	Scheme:      "http",
}

func TestNodesFromEndpointSlicesServingEndpointIsActive(t *testing.T) {
	active, lingering := nodesFromEndpointSlices([]*discoveryv1.EndpointSlice{
		newEndpointSlice(8000, servingEndpoint("agentenv-node-a", "10.0.0.1")),
	}, defaultDiscoveryCfg, nil)

	if len(active) != 1 {
		t.Fatalf("expected 1 active node, got %d", len(active))
	}
	if len(lingering) != 0 {
		t.Fatalf("expected 0 lingering nodes, got %d", len(lingering))
	}
	if got := active[0].ID; got != "agentenv-node-a" {
		t.Fatalf("expected node id agentenv-node-a, got %q", got)
	}
	if got := active[0].Endpoint; got != "http://10.0.0.1:8000" {
		t.Fatalf("expected endpoint http://10.0.0.1:8000, got %q", got)
	}
}

func TestNodesFromEndpointSlicesUsesPodNodeNameAsStableID(t *testing.T) {
	active, lingering := nodesFromEndpointSlices(
		[]*discoveryv1.EndpointSlice{
			newEndpointSlice(8000, servingEndpointWithUID("agentenv-node-a-abc", "pod-uid-a", "10.0.0.1")),
		},
		defaultDiscoveryCfg,
		map[string]podIdentity{
			"agentenv-node-a-abc": {uid: "pod-uid-a", nodeName: "worker-a"},
		},
	)

	if len(active) != 1 || len(lingering) != 0 {
		t.Fatalf("expected one active node, got active=%d lingering=%d", len(active), len(lingering))
	}
	if got := active[0].ID; got != "worker-a" {
		t.Fatalf("expected stable node id worker-a, got %q", got)
	}

	// A replacement Pod on the same host keeps the owner identity while its
	// endpoint is refreshed from the new Pod address.
	active, lingering = nodesFromEndpointSlices(
		[]*discoveryv1.EndpointSlice{
			newEndpointSlice(8000, servingEndpointWithUID("agentenv-node-a-def", "pod-uid-b", "10.0.0.2")),
		},
		defaultDiscoveryCfg,
		map[string]podIdentity{
			"agentenv-node-a-def": {uid: "pod-uid-b", nodeName: "worker-a"},
		},
	)
	if len(active) != 1 || active[0].ID != "worker-a" || active[0].Endpoint != "http://10.0.0.2:8000" {
		t.Fatalf("replacement Pod changed stable placement: active=%v", active)
	}
	if len(lingering) != 0 {
		t.Fatalf("replacement Pod should not be lingering, got %v", lingering)
	}
}

func TestNodesFromEndpointSlicesPrefersServingReplacementForStableNode(t *testing.T) {
	oldEndpoint := terminatingEndpointWithUID("agentenv-node-a-old", "pod-uid-old", "10.0.0.1")
	newEndpoint := servingEndpointWithUID("agentenv-node-a-new", "pod-uid-new", "10.0.0.2")
	podNodeIDs := map[string]podIdentity{
		"agentenv-node-a-old": {uid: "pod-uid-old", nodeName: "worker-a"},
		"agentenv-node-a-new": {uid: "pod-uid-new", nodeName: "worker-a"},
	}

	for _, endpoints := range [][]discoveryv1.Endpoint{{oldEndpoint, newEndpoint}, {newEndpoint, oldEndpoint}} {
		active, lingering := nodesFromEndpointSlices(
			[]*discoveryv1.EndpointSlice{newEndpointSlice(8000, endpoints...)},
			defaultDiscoveryCfg,
			podNodeIDs,
		)
		if len(active) != 1 || len(lingering) != 0 {
			t.Fatalf("serving replacement must win regardless of endpoint order: active=%v lingering=%v", active, lingering)
		}
		if got := active[0]; got.ID != "worker-a" || got.Endpoint != "http://10.0.0.2:8000" {
			t.Fatalf("unexpected serving replacement node: %+v", got)
		}
	}
}

func TestNodesFromEndpointSlicesFailsClosedForMultipleServingPodsOnStableNode(t *testing.T) {
	active, lingering := nodesFromEndpointSlices(
		[]*discoveryv1.EndpointSlice{newEndpointSlice(8000,
			servingEndpointWithUID("agentenv-node-a-old", "pod-uid-old", "10.0.0.1"),
			servingEndpointWithUID("agentenv-node-a-new", "pod-uid-new", "10.0.0.2"),
		)},
		defaultDiscoveryCfg,
		map[string]podIdentity{
			"agentenv-node-a-old": {uid: "pod-uid-old", nodeName: "worker-a"},
			"agentenv-node-a-new": {uid: "pod-uid-new", nodeName: "worker-a"},
		},
	)
	if len(active) != 0 || len(lingering) != 0 {
		t.Fatalf("ambiguous serving Pods must be excluded, got active=%v lingering=%v", active, lingering)
	}
}

func TestNodesFromEndpointSlicesCarriesServingPodUID(t *testing.T) {
	active, lingering := nodesFromEndpointSlices(
		[]*discoveryv1.EndpointSlice{newEndpointSlice(8000,
			servingEndpointWithUID("agentenv-node-a", "pod-uid-a", "10.0.0.1"),
		)},
		defaultDiscoveryCfg,
		map[string]podIdentity{
			"agentenv-node-a": {uid: "pod-uid-a", nodeName: "worker-a"},
		},
	)
	if len(active) != 1 || len(lingering) != 0 {
		t.Fatalf("expected one active node, got active=%v lingering=%v", active, lingering)
	}
	if got := active[0].ServiceInstanceID; got != "pod-uid-a" {
		t.Fatalf("expected discovered service instance pod-uid-a, got %q", got)
	}
}

func TestNodesFromEndpointSlicesRejectsStalePodUID(t *testing.T) {
	active, lingering := nodesFromEndpointSlices(
		[]*discoveryv1.EndpointSlice{newEndpointSlice(8000,
			servingEndpointWithUID("agentenv-node-a", "old-pod-uid", "10.0.0.1"),
		)},
		defaultDiscoveryCfg,
		map[string]podIdentity{
			"agentenv-node-a": {uid: "current-pod-uid", nodeName: "worker-a"},
		},
	)
	if len(active) != 0 || len(lingering) != 0 {
		t.Fatalf("stale EndpointSlice target must be excluded, got active=%v lingering=%v", active, lingering)
	}
}

func TestNodesFromEndpointSlicesRejectsMissingPodUID(t *testing.T) {
	active, lingering := nodesFromEndpointSlices(
		[]*discoveryv1.EndpointSlice{newEndpointSlice(8000,
			servingEndpoint("agentenv-node-a", "10.0.0.1"),
		)},
		defaultDiscoveryCfg,
		map[string]podIdentity{
			"agentenv-node-a": {uid: "current-pod-uid", nodeName: "worker-a"},
		},
	)
	if len(active) != 0 || len(lingering) != 0 {
		t.Fatalf("EndpointSlice without target UID must be excluded, got active=%v lingering=%v", active, lingering)
	}
}

func TestPodNodeIDsFromStoreKeepsUIDAndNodeName(t *testing.T) {
	pod := newPod("agentenv-node-a", nil)
	pod.UID = types.UID("pod-uid-a")
	pod.Spec.NodeName = "worker-a"
	missingUID := newPod("agentenv-node-missing-uid", nil)
	missingUID.Spec.NodeName = "worker-a"
	missingNode := newPod("agentenv-node-missing-node", nil)
	missingNode.UID = types.UID("pod-uid-missing-node")
	identities := podNodeIDsFromStore([]interface{}{pod, missingUID, missingNode})

	got, ok := identities[pod.Name]
	if !ok {
		t.Fatal("expected Pod identity in informer map")
	}
	if got.uid != "pod-uid-a" || got.nodeName != "worker-a" {
		t.Fatalf("unexpected Pod identity: %+v", got)
	}
	if _, ok := identities[missingUID.Name]; ok {
		t.Fatal("Pod without UID must not be considered routable")
	}
	if _, ok := identities[missingNode.Name]; ok {
		t.Fatal("Pod without nodeName must not be considered routable")
	}
}

func TestFilterNodesByPodLabelsUsesStablePodNodeName(t *testing.T) {
	pod := newPod("agentenv-node-a-abc", map[string]string{"agentenv.io/scheduler-state": "no-schedule"})
	pod.Spec.NodeName = "worker-a"
	discovery := newDiscoveryWithPodLabelSelectors(t, "", "agentenv.io/scheduler-state=no-schedule", pod)

	active, lingering := discovery.filterNodesByPodLabels(
		[]Node{{ID: "worker-a", Endpoint: "http://10.0.0.1:8000"}},
		nil,
	)
	if len(active) != 0 || len(lingering) != 1 || lingering[0].ID != "worker-a" {
		t.Fatalf("expected stable worker-a to become lingering, got active=%v lingering=%v", active, lingering)
	}
}

func TestNodesFromEndpointSlicesNotServingIsExcluded(t *testing.T) {
	active, lingering := nodesFromEndpointSlices([]*discoveryv1.EndpointSlice{
		newEndpointSlice(8000, notServingEndpoint("agentenv-node-a", "10.0.0.1")),
	}, defaultDiscoveryCfg, nil)

	if len(active) != 0 {
		t.Fatalf("expected 0 active nodes, got %d", len(active))
	}
	if len(lingering) != 0 {
		t.Fatalf("expected 0 lingering nodes, got %d", len(lingering))
	}
}

func TestNodesFromEndpointSlicesTerminatingIsLingering(t *testing.T) {
	active, lingering := nodesFromEndpointSlices([]*discoveryv1.EndpointSlice{
		newEndpointSlice(8000, terminatingEndpoint("agentenv-node-a", "10.0.0.1")),
	}, defaultDiscoveryCfg, nil)

	if len(active) != 0 {
		t.Fatalf("expected 0 active nodes, got %d", len(active))
	}
	if len(lingering) != 1 {
		t.Fatalf("expected 1 lingering node, got %d", len(lingering))
	}
	if got := lingering[0].ID; got != "agentenv-node-a" {
		t.Fatalf("expected lingering node agentenv-node-a, got %q", got)
	}
}

func TestFilterNodesByPodLabelsNoScheduleIsLingering(t *testing.T) {
	pod := newPod("agentenv-node-a", map[string]string{"agentenv.io/scheduler-state": "no-schedule"})
	pod.Spec.NodeName = "agentenv-node-a"
	discovery := newDiscoveryWithPodLabelSelectors(t, "", "agentenv.io/scheduler-state=no-schedule", pod)

	active, lingering := discovery.filterNodesByPodLabels(
		[]Node{{ID: "agentenv-node-a", Endpoint: "http://10.0.0.1:8000"}},
		nil,
	)

	if len(active) != 0 {
		t.Fatalf("expected 0 active nodes, got %d", len(active))
	}
	if len(lingering) != 1 {
		t.Fatalf("expected 1 lingering node, got %d", len(lingering))
	}
	if got := lingering[0].ID; got != "agentenv-node-a" {
		t.Fatalf("expected lingering node agentenv-node-a, got %q", got)
	}
}

func TestFilterNodesByPodLabelsIgnoreTakesPrecedence(t *testing.T) {
	pod := newPod("agentenv-node-a", map[string]string{
		"agentenv.io/discovery":       "ignore",
		"agentenv.io/scheduler-state": "no-schedule",
	})
	pod.Spec.NodeName = "agentenv-node-a"
	discovery := newDiscoveryWithPodLabelSelectors(t, "agentenv.io/discovery=ignore", "agentenv.io/scheduler-state=no-schedule", pod)

	active, lingering := discovery.filterNodesByPodLabels(
		[]Node{{ID: "agentenv-node-a", Endpoint: "http://10.0.0.1:8000"}},
		[]Node{{ID: "agentenv-node-a", Endpoint: "http://10.0.0.1:8000"}},
	)

	if len(active) != 0 || len(lingering) != 0 {
		t.Fatalf("expected ignored pod to be excluded, got active=%d lingering=%d", len(active), len(lingering))
	}
}

func TestParseOptionalPodSelector(t *testing.T) {
	if selector, err := parseOptionalPodSelector("", "ignore_pod_selector"); err != nil || selector != nil {
		t.Fatalf("expected empty selector to be nil and valid, got selector=%v err=%v", selector, err)
	}
	if selector, err := parseOptionalPodSelector("agentenv.io/scheduler-state in (draining,no-schedule)", "no_schedule_pod_selector"); err != nil || selector == nil {
		t.Fatalf("expected selector to be valid, got selector=%v err=%v", selector, err)
	}
	if _, err := parseOptionalPodSelector("agentenv.io/scheduler-state in (", "no_schedule_pod_selector"); err == nil {
		t.Fatal("expected invalid selector to be rejected")
	}
}

func TestNodesFromEndpointSlicesNotServingTerminatingIsExcluded(t *testing.T) {
	ep := discoveryv1.Endpoint{
		Addresses: []string{"10.0.0.1"},
		Conditions: discoveryv1.EndpointConditions{
			Serving:     boolPtr(false),
			Terminating: boolPtr(true),
		},
		TargetRef: &corev1.ObjectReference{Kind: "Pod", Name: "agentenv-node-a"},
	}
	active, lingering := nodesFromEndpointSlices([]*discoveryv1.EndpointSlice{
		newEndpointSlice(8000, ep),
	}, defaultDiscoveryCfg, nil)

	if len(active)+len(lingering) != 0 {
		t.Fatalf("expected no nodes for not-serving+terminating, got active=%d lingering=%d", len(active), len(lingering))
	}
}

func TestNodesFromEndpointSlicesFormatsIPv6Endpoint(t *testing.T) {
	active, _ := nodesFromEndpointSlices([]*discoveryv1.EndpointSlice{
		newEndpointSlice(8000, servingEndpoint("agentenv-node-v6", "2001:db8::10")),
	}, defaultDiscoveryCfg, nil)

	if len(active) != 1 {
		t.Fatalf("expected 1 node, got %d", len(active))
	}
	if got := active[0].Endpoint; got != "http://[2001:db8::10]:8000" {
		t.Fatalf("expected endpoint http://[2001:db8::10]:8000, got %q", got)
	}
}

func TestNodesFromEndpointSlicesSkipsInvalidAddresses(t *testing.T) {
	active, _ := nodesFromEndpointSlices([]*discoveryv1.EndpointSlice{
		newEndpointSlice(8000, endpointWithAddresses("agentenv-node-a", []string{"not-an-ip"})),
	}, defaultDiscoveryCfg, nil)

	if len(active) != 0 {
		t.Fatalf("expected no nodes, got %d", len(active))
	}
}

func TestNodesFromEndpointSlicesUsesFirstValidAddress(t *testing.T) {
	active, _ := nodesFromEndpointSlices([]*discoveryv1.EndpointSlice{
		newEndpointSlice(8000, endpointWithAddresses("agentenv-node-a", []string{"not-an-ip", "10.0.0.3"})),
	}, defaultDiscoveryCfg, nil)

	if len(active) != 1 {
		t.Fatalf("expected 1 node, got %d", len(active))
	}
	if got := active[0].Endpoint; got != "http://10.0.0.3:8000" {
		t.Fatalf("expected endpoint http://10.0.0.3:8000, got %q", got)
	}
}

func TestNodesFromEndpointSlicesIgnoresSlicesWithoutMatchingPort(t *testing.T) {
	active, _ := nodesFromEndpointSlices([]*discoveryv1.EndpointSlice{
		newEndpointSlice(9000, servingEndpoint("agentenv-node-a", "10.0.0.1")),
	}, defaultDiscoveryCfg, nil)

	if len(active) != 0 {
		t.Fatalf("expected no nodes, got %d", len(active))
	}
}

func TestNodeRegistryReflectsEndpointRemovalAcrossSyncs(t *testing.T) {
	registry := NewAtomicNodeRegistry(nil, 30*time.Second)
	now := time.Unix(100, 0)

	active1, _ := nodesFromEndpointSlices([]*discoveryv1.EndpointSlice{
		newEndpointSlice(8000,
			servingEndpoint("agentenv-node-a", "10.0.0.1"),
			servingEndpoint("agentenv-node-b", "10.0.0.2"),
		),
	}, defaultDiscoveryCfg, nil)
	registry.Set(active1, nil)
	// Both are active; heartbeat them so they become ready for Snapshot.
	heartbeatNode(registry, "agentenv-node-a", "http://10.0.0.1:8000", now)
	heartbeatNode(registry, "agentenv-node-b", "http://10.0.0.2:8000", now)

	if got := len(registry.Snapshot( /* allowLingering */ false)); got != 2 {
		t.Fatalf("expected 2 nodes after initial sync, got %d", got)
	}

	active2, _ := nodesFromEndpointSlices([]*discoveryv1.EndpointSlice{
		newEndpointSlice(8000, servingEndpoint("agentenv-node-b", "10.0.0.2")),
	}, defaultDiscoveryCfg, nil)
	registry.Set(active2, nil)

	snapshot := registry.Snapshot( /* allowLingering */ false)
	if len(snapshot) != 1 {
		t.Fatalf("expected 1 node after removal sync, got %d", len(snapshot))
	}
	if got := snapshot[0].ID; got != "agentenv-node-b" {
		t.Fatalf("expected remaining node agentenv-node-b, got %q", got)
	}
}

func TestSnapshotFiltersLingeringNodes(t *testing.T) {
	registry := NewAtomicNodeRegistry(nil, 30*time.Second)

	// node-a: active
	// node-b: active
	// node-c: lingering
	active := []Node{
		{ID: "node-a", Endpoint: "http://node-a"},
		{ID: "node-b", Endpoint: "http://node-b"},
	}
	lingering := []Node{
		{ID: "node-c", Endpoint: "http://node-c"},
	}
	registry.Set(active, lingering)

	// Without lingering: only active nodes
	noLingering := registry.Snapshot( /* allowLingering */ false)
	if len(noLingering) != 2 {
		t.Fatalf("expected 2 active nodes, got %v", nodeIDs(noLingering))
	}

	// With lingering: all nodes
	withLingering := registry.Snapshot( /* allowLingering */ true)
	if len(withLingering) != 3 {
		t.Fatalf("expected 3 nodes with lingering, got %v", nodeIDs(withLingering))
	}
}

func TestLingeringNodeGetsNoScheduleStatusInObservedView(t *testing.T) {
	registry := NewAtomicNodeRegistry(nil, 30*time.Second)
	now := time.Unix(100, 0)

	registry.Set(nil, []Node{{ID: "node-a", Endpoint: "http://node-a"}})
	heartbeatNode(registry, "node-a", "http://node-a", now)

	observed, ok := registry.GetObserved("node-a", "", now)
	if !ok {
		t.Fatal("expected observed node")
	}
	if got := observed.GetSnapshot().GetStatus(); got != schedulerv1.NodeStatus_NODE_STATUS_LINGERING {
		t.Fatalf("expected NO_SCHEDULE for lingering node, got %v", got)
	}
}

func TestActiveNodeGetsReadyStatusInObservedView(t *testing.T) {
	registry := NewAtomicNodeRegistry(nil, 30*time.Second)
	now := time.Unix(100, 0)

	registry.Set([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, nil)
	registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         "cluster-a",
		ServiceInstanceId: "svc-a",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
	}, now)

	observed, ok := registry.GetObserved("node-a", "", now)
	if !ok {
		t.Fatal("expected observed node")
	}
	if got := observed.GetSnapshot().GetStatus(); got != schedulerv1.NodeStatus_NODE_STATUS_READY {
		t.Fatalf("expected READY for active node, got %v", got)
	}
}

func TestSetRemovesObservedNodesMissingFromDiscovery(t *testing.T) {
	registry := NewAtomicNodeRegistry(nil, 30*time.Second)
	now := time.Unix(100, 0)

	registry.Set([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, nil)
	heartbeatNode(registry, "node-a", "http://node-a", now)
	registry.Set(nil, nil) // remove from discovery

	if _, ok := registry.GetObserved("node-a", "", now); ok {
		t.Fatal("expected observed node to be removed from registry")
	}
	if nodes := registry.ListObserved("", now); len(nodes) != 0 {
		t.Fatalf("expected no observed nodes after removal, got %d", len(nodes))
	}
}

// ── test helpers ──

func heartbeatNode(registry *AtomicNodeRegistry, nodeID, endpoint string, now time.Time) {
	registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            nodeID,
		ClusterId:         "cluster-test",
		ServiceInstanceId: "svc-" + nodeID,
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
	}, now)
}

func nodeIDs(nodes []Node) []string {
	ids := make([]string, len(nodes))
	for i, n := range nodes {
		ids[i] = n.ID
	}
	return ids
}

func newDiscoveryWithPodLabelSelectors(t *testing.T, ignoreSelector string, noScheduleSelector string, pods ...*corev1.Pod) *KubernetesDiscovery {
	t.Helper()

	ignore, err := parseOptionalPodSelector(ignoreSelector, "ignore_pod_selector")
	if err != nil {
		t.Fatalf("parse ignore selector: %v", err)
	}
	noSchedule, err := parseOptionalPodSelector(noScheduleSelector, "no_schedule_pod_selector")
	if err != nil {
		t.Fatalf("parse no-schedule selector: %v", err)
	}

	return &KubernetesDiscovery{
		config:                defaultDiscoveryCfg,
		podInformer:           newPodStoreInformer(t, pods...),
		ignorePodSelector:     ignore,
		noSchedulePodSelector: noSchedule,
	}
}

func newPodStoreInformer(t *testing.T, pods ...*corev1.Pod) cache.SharedIndexInformer {
	t.Helper()

	informer := cache.NewSharedIndexInformer(&cache.ListWatch{}, &corev1.Pod{}, 0, cache.Indexers{})
	for _, pod := range pods {
		if err := informer.GetStore().Add(pod); err != nil {
			t.Fatalf("add pod to informer store: %v", err)
		}
	}
	return informer
}

func newPod(name string, labels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: defaultDiscoveryCfg.Namespace,
			Labels:    labels,
		},
	}
}

func newEndpointSlice(port int32, endpoints ...discoveryv1.Endpoint) *discoveryv1.EndpointSlice {
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "agentenv-nodes-slice",
			Namespace: "agentenv-system",
			Labels: map[string]string{
				discoveryv1.LabelServiceName: "agentenv-nodes",
			},
		},
		Ports: []discoveryv1.EndpointPort{
			{Port: int32Ptr(port)},
		},
		Endpoints: endpoints,
	}
}

func servingEndpoint(name string, address string) discoveryv1.Endpoint {
	return servingEndpointWithUID(name, "", address)
}

func servingEndpointWithUID(name string, uid string, address string) discoveryv1.Endpoint {
	return discoveryv1.Endpoint{
		Addresses: []string{address},
		Conditions: discoveryv1.EndpointConditions{
			Serving: boolPtr(true),
		},
		TargetRef: &corev1.ObjectReference{
			Kind: "Pod",
			Name: name,
			UID:  types.UID(uid),
		},
	}
}

func notServingEndpoint(name string, address string) discoveryv1.Endpoint {
	return discoveryv1.Endpoint{
		Addresses: []string{address},
		Conditions: discoveryv1.EndpointConditions{
			Serving: boolPtr(false),
		},
		TargetRef: &corev1.ObjectReference{
			Kind: "Pod",
			Name: name,
		},
	}
}

func terminatingEndpoint(name string, address string) discoveryv1.Endpoint {
	return terminatingEndpointWithUID(name, "", address)
}

func terminatingEndpointWithUID(name string, uid string, address string) discoveryv1.Endpoint {
	return discoveryv1.Endpoint{
		Addresses: []string{address},
		Conditions: discoveryv1.EndpointConditions{
			Serving:     boolPtr(true),
			Terminating: boolPtr(true),
		},
		TargetRef: &corev1.ObjectReference{
			Kind: "Pod",
			Name: name,
			UID:  types.UID(uid),
		},
	}
}

func endpointWithAddresses(name string, addresses []string) discoveryv1.Endpoint {
	return discoveryv1.Endpoint{
		Addresses: addresses,
		Conditions: discoveryv1.EndpointConditions{
			Serving: boolPtr(true),
		},
		TargetRef: &corev1.ObjectReference{
			Kind: "Pod",
			Name: name,
		},
	}
}

func int32Ptr(v int32) *int32 {
	return &v
}

func boolPtr(v bool) *bool {
	return &v
}
