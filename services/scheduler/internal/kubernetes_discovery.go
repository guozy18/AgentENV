package scheduler

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"sort"
	"strconv"
	"strings"
	"time"

	"agentenv/services/shared/config"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	"go.uber.org/zap"
)

const kubernetesDiscoveryCacheSyncTimeout = 30 * time.Second

type KubernetesDiscovery struct {
	logger                *zap.Logger
	config                config.SchedulerDiscoveryKubernetesConfig
	registry              *AtomicNodeRegistry
	endpointSliceInformer cache.SharedIndexInformer
	podInformer           cache.SharedIndexInformer
	ignorePodSelector     labels.Selector
	noSchedulePodSelector labels.Selector
}

func NewKubernetesDiscovery(
	logger *zap.Logger,
	cfg config.SchedulerDiscoveryKubernetesConfig,
	registry *AtomicNodeRegistry,
) (*KubernetesDiscovery, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	if registry == nil {
		return nil, fmt.Errorf("registry is required")
	}

	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("load in-cluster kubernetes config: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("build kubernetes client: %w", err)
	}

	ignorePodSelector, err := parseOptionalPodSelector(cfg.IgnorePodSelector, "ignore_pod_selector")
	if err != nil {
		return nil, err
	}
	noSchedulePodSelector, err := parseOptionalPodSelector(cfg.NoSchedulePodSelector, "no_schedule_pod_selector")
	if err != nil {
		return nil, err
	}

	endpointSliceFactory := informers.NewSharedInformerFactoryWithOptions(
		clientset,
		0,
		informers.WithNamespace(cfg.Namespace),
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = discoveryv1.LabelServiceName + "=" + cfg.ServiceName
		}),
	)
	endpointSliceInformer := endpointSliceFactory.Discovery().V1().EndpointSlices().Informer()
	podFactory := informers.NewSharedInformerFactoryWithOptions(
		clientset,
		0,
		informers.WithNamespace(cfg.Namespace),
	)
	podInformer := podFactory.Core().V1().Pods().Informer()

	discovery := &KubernetesDiscovery{
		logger:                logger,
		config:                cfg,
		registry:              registry,
		endpointSliceInformer: endpointSliceInformer,
		podInformer:           podInformer,
		ignorePodSelector:     ignorePodSelector,
		noSchedulePodSelector: noSchedulePodSelector,
	}
	addSyncEventHandler(endpointSliceInformer, discovery.syncFromStore)
	addSyncEventHandler(podInformer, discovery.syncFromStore)

	return discovery, nil
}

func parseOptionalPodSelector(raw string, field string) (labels.Selector, error) {
	selector := strings.TrimSpace(raw)
	if selector == "" {
		return nil, nil
	}
	parsed, err := labels.Parse(selector)
	if err != nil {
		return nil, fmt.Errorf("scheduler.discovery.kubernetes.%s is invalid: %w", field, err)
	}
	return parsed, nil
}

func addSyncEventHandler(informer cache.SharedIndexInformer, sync func()) {
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(_ interface{}) {
			sync()
		},
		UpdateFunc: func(_, _ interface{}) {
			sync()
		},
		DeleteFunc: func(_ interface{}) {
			sync()
		},
	})
}

func (d *KubernetesDiscovery) Run(ctx context.Context) error {
	go d.endpointSliceInformer.Run(ctx.Done())
	go d.podInformer.Run(ctx.Done())

	syncCtx, cancelSync := context.WithTimeout(ctx, kubernetesDiscoveryCacheSyncTimeout)
	defer cancelSync()
	if !cache.WaitForCacheSync(syncCtx.Done(), d.endpointSliceInformer.HasSynced, d.podInformer.HasSynced) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := syncCtx.Err(); err != nil {
			return fmt.Errorf("kubernetes discovery cache sync timed out after %s", kubernetesDiscoveryCacheSyncTimeout)
		}
		return fmt.Errorf("kubernetes discovery cache sync failed")
	}

	d.syncFromStore()

	<-ctx.Done()
	return ctx.Err()
}

func (d *KubernetesDiscovery) syncFromStore() {
	if !d.cacheSynced() {
		return
	}

	objects := d.endpointSliceInformer.GetStore().List()
	podNodeIDs := podNodeIDsFromStore(d.podInformer.GetStore().List())
	active, lingering := nodesFromEndpointSliceObjectsWithPodNodeIDs(objects, d.config, podNodeIDs)
	active, lingering = d.filterNodesByPodLabels(active, lingering)
	d.registry.Set(active, lingering)

	d.logger.Debug("scheduler refreshed kubernetes-discovered nodes",
		zap.String("namespace", d.config.Namespace),
		zap.String("service_name", d.config.ServiceName),
		zap.Int("active_count", len(active)),
		zap.Int("lingering_count", len(lingering)),
	)
}

func (d *KubernetesDiscovery) cacheSynced() bool {
	return d.endpointSliceInformer.HasSynced() && d.podInformer.HasSynced()
}

func nodesFromEndpointSliceObjectsWithPodNodeIDs(
	objects []interface{},
	cfg config.SchedulerDiscoveryKubernetesConfig,
	podNodeIDs map[string]podIdentity,
) ([]Node, []Node) {
	slices := make([]*discoveryv1.EndpointSlice, 0, len(objects))
	for _, object := range objects {
		slice, ok := object.(*discoveryv1.EndpointSlice)
		if !ok || slice == nil {
			continue
		}
		slices = append(slices, slice)
	}
	return nodesFromEndpointSlices(slices, cfg, podNodeIDs)
}

func nodesFromEndpointSlices(
	slices []*discoveryv1.EndpointSlice,
	cfg config.SchedulerDiscoveryKubernetesConfig,
	podNodeIDs map[string]podIdentity,
) (active []Node, lingering []Node) {
	if len(slices) == 0 {
		return nil, nil
	}

	scheme := strings.TrimSpace(cfg.Scheme)
	if scheme == "" {
		scheme = "http"
	}

	type nodeCandidates struct {
		active    map[string]Node
		lingering map[string]Node
	}
	candidatesByNodeID := make(map[string]*nodeCandidates)
	for _, slice := range slices {
		if slice == nil {
			continue
		}
		if !hasMatchingEndpointPort(slice.Ports, cfg.Port) {
			continue
		}
		for _, endpoint := range slice.Endpoints {
			node, lingering, ok := nodeFromEndpointWithPodNodeIDs(endpoint, cfg.Port, scheme, podNodeIDs)
			if !ok {
				continue
			}
			candidates := candidatesByNodeID[node.ID]
			if candidates == nil {
				candidates = &nodeCandidates{
					active:    make(map[string]Node),
					lingering: make(map[string]Node),
				}
				candidatesByNodeID[node.ID] = candidates
			}
			serviceInstanceID := node.ServiceInstanceID
			if lingering {
				candidates.lingering[serviceInstanceID] = chooseDiscoveredNodeEndpoint(candidates.lingering[serviceInstanceID], node)
			} else {
				candidates.active[serviceInstanceID] = chooseDiscoveredNodeEndpoint(candidates.active[serviceInstanceID], node)
			}
		}
	}

	for _, candidates := range candidatesByNodeID {
		switch len(candidates.active) {
		case 1:
			// A serving replacement wins over any terminating endpoint. This is
			// safe because the serving candidate is the sole active Pod identity.
			for _, node := range candidates.active {
				active = append(active, node)
			}
		case 0:
			if len(candidates.lingering) == 1 {
				for _, node := range candidates.lingering {
					lingering = append(lingering, node)
				}
			}
		default:
			// Multiple simultaneously serving Pods mapped to one host are an
			// ambiguous owner/endpoint state. Fail closed until discovery settles
			// instead of routing snapshots to an arbitrary Pod.
			continue
		}
	}
	sort.Slice(active, func(i, j int) bool { return active[i].ID < active[j].ID })
	sort.Slice(lingering, func(i, j int) bool { return lingering[i].ID < lingering[j].ID })
	return
}

func chooseDiscoveredNodeEndpoint(existing Node, candidate Node) Node {
	if existing.ID == "" || candidate.Endpoint < existing.Endpoint {
		return candidate
	}
	return existing
}

func hasMatchingEndpointPort(ports []discoveryv1.EndpointPort, port int32) bool {
	for _, candidate := range ports {
		if candidate.Port != nil && *candidate.Port == port {
			return true
		}
	}
	return false
}

func (d *KubernetesDiscovery) filterNodesByPodLabels(active []Node, lingering []Node) ([]Node, []Node) {
	if d.ignorePodSelector == nil && d.noSchedulePodSelector == nil {
		return active, lingering
	}

	filteredActive := make([]Node, 0, len(active))
	filteredLingering := make([]Node, 0, len(active)+len(lingering))
	for _, node := range active {
		if node.ID != "" && objectMatchesForNode(d.podInformer, d.ignorePodSelector, node.ID) {
			continue
		}
		if node.ID != "" && objectMatchesForNode(d.podInformer, d.noSchedulePodSelector, node.ID) {
			filteredLingering = append(filteredLingering, node)
			continue
		}
		filteredActive = append(filteredActive, node)
	}

	for _, node := range lingering {
		if node.ID != "" && objectMatchesForNode(d.podInformer, d.ignorePodSelector, node.ID) {
			continue
		}
		filteredLingering = append(filteredLingering, node)
	}

	return filteredActive, filteredLingering
}

func objectMatchesForNode(informer cache.SharedIndexInformer, selector labels.Selector, nodeID string) bool {
	if informer == nil || selector == nil {
		return false
	}
	for _, object := range informer.GetStore().List() {
		pod, ok := object.(*corev1.Pod)
		if ok && pod != nil && strings.TrimSpace(pod.Spec.NodeName) == nodeID && selector.Matches(labels.Set(pod.Labels)) {
			return true
		}
	}
	return false
}

// podIdentity is the informer-side identity for a node Pod.  EndpointSlice
// target names are not sufficient to fence a replaced Pod: Kubernetes can
// briefly expose an old endpoint while a Pod with the same logical role is
// being created.  The UID is the immutable identity that must match the
// EndpointSlice TargetRef.UID before we route traffic to it.
type podIdentity struct {
	uid      string
	nodeName string
}

func podNodeIDsFromStore(objects []interface{}) map[string]podIdentity {
	identities := make(map[string]podIdentity, len(objects))
	for _, object := range objects {
		pod, ok := object.(*corev1.Pod)
		if !ok || pod == nil || strings.TrimSpace(pod.Name) == "" {
			continue
		}
		uid := strings.TrimSpace(string(pod.UID))
		nodeID := strings.TrimSpace(pod.Spec.NodeName)
		// A Pod without either identity is not a routable production
		// candidate.  Keeping it out of the map makes the production path
		// fail closed while the informer catches up.
		if uid == "" || nodeID == "" {
			continue
		}
		identities[pod.Name] = podIdentity{uid: uid, nodeName: nodeID}
	}
	return identities
}

func nodeFromEndpointWithPodNodeIDs(
	endpoint discoveryv1.Endpoint,
	port int32,
	scheme string,
	podNodeIDs map[string]podIdentity,
) (node Node, lingering bool, ok bool) {
	serving := endpoint.Conditions.Serving != nil && *endpoint.Conditions.Serving
	if !serving {
		return Node{}, false, false
	}

	if endpoint.TargetRef == nil || endpoint.TargetRef.Name == "" {
		return Node{}, false, false
	}

	podName := strings.TrimSpace(endpoint.TargetRef.Name)
	nodeID := podName
	serviceInstanceID := strings.TrimSpace(string(endpoint.TargetRef.UID))
	if podNodeIDs != nil {
		pod, found := podNodeIDs[podName]
		if !found || pod.nodeName == "" || pod.uid == "" || serviceInstanceID == "" || serviceInstanceID != pod.uid {
			return Node{}, false, false
		}
		nodeID = pod.nodeName
	}

	address, addrOK := selectRoutableEndpointAddress(endpoint.Addresses)
	if !addrOK {
		return Node{}, false, false
	}

	terminating := endpoint.Conditions.Terminating != nil && *endpoint.Conditions.Terminating

	hostPort := net.JoinHostPort(address, strconv.Itoa(int(port)))
	return Node{
		ID:                strings.TrimSpace(nodeID),
		Endpoint:          fmt.Sprintf("%s://%s", scheme, hostPort),
		ServiceInstanceID: serviceInstanceID,
	}, terminating, true
}

func selectRoutableEndpointAddress(addresses []string) (string, bool) {
	for _, candidate := range addresses {
		address := strings.TrimSpace(candidate)
		if address == "" {
			continue
		}
		if _, err := netip.ParseAddr(address); err != nil {
			continue
		}
		return address, true
	}
	return "", false
}
