// Package enrich attaches Kubernetes metadata to log records using a
// node-scoped Pod informer. Scoping the informer to this node's pods
// (fieldSelector spec.nodeName=$NODE) keeps the cache small.
package enrich

import (
	"context"
	"sync"
	"time"

	"github.com/andyleap/simplelog/internal/model"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

// Enricher maintains a UID->metadata cache fed by a node-scoped Pod informer.
type Enricher struct {
	mu          sync.RWMutex
	byUID       map[string]podMeta
	excludeAnno string // pods with this annotation set to "false" are not ingested
}

type podMeta struct {
	labels      map[string]string
	annotations map[string]string
	node        string
	images      map[string]string // container name -> image
}

// New builds an Enricher and starts the informer, blocking until the cache has
// synced or ctx is cancelled. excludeAnno is the pod annotation key whose value
// "false" opts a pod out of log ingestion (empty disables annotation filtering).
func New(ctx context.Context, client kubernetes.Interface, nodeName, excludeAnno string) (*Enricher, error) {
	e := &Enricher{byUID: map[string]podMeta{}, excludeAnno: excludeAnno}

	factory := informers.NewSharedInformerFactoryWithOptions(
		client, 10*time.Minute,
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.FieldSelector = "spec.nodeName=" + nodeName
		}),
	)
	informer := factory.Core().V1().Pods().Informer()
	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { e.upsert(obj) },
		UpdateFunc: func(_, obj any) { e.upsert(obj) },
		DeleteFunc: func(obj any) { e.remove(obj) },
	})
	if err != nil {
		return nil, err
	}

	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		return nil, ctx.Err()
	}
	return e, nil
}

// NewInCluster builds an Enricher using the in-cluster ServiceAccount config.
func NewInCluster(ctx context.Context, nodeName, excludeAnno string) (*Enricher, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return New(ctx, client, nodeName, excludeAnno)
}

// Ready reports whether the informer has metadata for the pod with the given
// UID. The tailer waits for this before forwarding a pod's logs so records are
// never shipped un-enriched (and the exclude annotation is known first).
func (e *Enricher) Ready(uid string) bool {
	e.mu.RLock()
	_, ok := e.byUID[uid]
	e.mu.RUnlock()
	return ok
}

// ShouldIngest reports whether logs for the pod with the given UID should be
// ingested. A pod is excluded when it carries the configured exclude annotation
// with value "false". Unknown pods (informer not yet synced) default to true.
func (e *Enricher) ShouldIngest(uid string) bool {
	if e.excludeAnno == "" {
		return true
	}
	e.mu.RLock()
	m, ok := e.byUID[uid]
	e.mu.RUnlock()
	if !ok {
		return true
	}
	return m.annotations[e.excludeAnno] != "false"
}

func (e *Enricher) upsert(obj any) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}
	images := map[string]string{}
	for _, c := range pod.Spec.Containers {
		images[c.Name] = c.Image
	}
	// Prefer the running image from status when available.
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Image != "" {
			images[cs.Name] = cs.Image
		}
	}
	e.mu.Lock()
	e.byUID[string(pod.UID)] = podMeta{
		labels:      pod.Labels,
		annotations: pod.Annotations,
		node:        pod.Spec.NodeName,
		images:      images,
	}
	e.mu.Unlock()
}

func (e *Enricher) remove(obj any) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		// Could be a DeletedFinalStateUnknown tombstone; ignore.
		return
	}
	e.mu.Lock()
	delete(e.byUID, string(pod.UID))
	e.mu.Unlock()
}

// Enrich attaches labels, annotations, node, and image to rec for the given
// pod UID. Unknown UIDs leave rec unchanged (path-derived fields remain).
func (e *Enricher) Enrich(rec *model.Record, uid string) {
	e.mu.RLock()
	m, ok := e.byUID[uid]
	e.mu.RUnlock()
	if !ok {
		return
	}
	if len(m.labels) > 0 {
		rec.Labels = m.labels
	}
	if len(m.annotations) > 0 {
		rec.Annotations = m.annotations
	}
	if m.node != "" {
		rec.Node = m.node
	}
	if img := m.images[rec.Container]; img != "" {
		rec.Image = img
	}
}
