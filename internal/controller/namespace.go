package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logr "sigs.k8s.io/controller-runtime/pkg/log"
)

// +kubebuilder:rbac:groups=core,resources=namespaces,verbs=get;list;watch

// NamespaceFilter decides whether a namespace should be watched. A namespace
// is selected when its name is in the explicit list OR its labels match the
// label selector (OR / union semantics).
type NamespaceFilter struct {
	names    map[string]struct{}
	selector labels.Selector
}

// NewNamespaceFilter builds a filter from an explicit namespace list and a
// standard Kubernetes label selector string. An empty labelSelector means no
// selector; an invalid one returns an error.
func NewNamespaceFilter(namespaces []string, labelSelector string) (*NamespaceFilter, error) {
	f := &NamespaceFilter{names: make(map[string]struct{}, len(namespaces))}
	for _, ns := range namespaces {
		if ns != "" {
			f.names[ns] = struct{}{}
		}
	}
	if labelSelector != "" {
		sel, err := labels.Parse(labelSelector)
		if err != nil {
			return nil, err
		}
		f.selector = sel
	}
	return f, nil
}

// Enabled reports whether any namespace restriction is configured.
func (f *NamespaceFilter) Enabled() bool {
	return len(f.names) > 0 || f.selector != nil
}

// UsesLabelSelector reports whether a label selector is configured. Only then
// does the watched set change at runtime, so only then is the NamespaceWatcher
// needed.
func (f *NamespaceFilter) UsesLabelSelector() bool {
	return f.selector != nil
}

// Matches reports whether the namespace with the given name and labels should
// be watched.
func (f *NamespaceFilter) Matches(name string, nsLabels map[string]string) bool {
	if !f.Enabled() {
		return true
	}
	if _, ok := f.names[name]; ok {
		return true
	}
	return f.selector != nil && f.selector.Matches(labels.Set(nsLabels))
}

// WatchedNamespaces resolves the set of namespace names to watch: the explicit
// names unioned with namespaces whose labels match the selector (listed via c).
// It is used at startup to build the namespace-scoped cache.
func (f *NamespaceFilter) WatchedNamespaces(ctx context.Context, c client.Client) (map[string]struct{}, error) {
	set := make(map[string]struct{}, len(f.names))
	for name := range f.names {
		set[name] = struct{}{}
	}
	if f.selector != nil {
		list := &corev1.NamespaceList{}
		if err := c.List(ctx, list, client.MatchingLabelsSelector{Selector: f.selector}); err != nil {
			return nil, err
		}
		for i := range list.Items {
			set[list.Items[i].Name] = struct{}{}
		}
	}
	return set, nil
}

// NamespaceWatcher watches Namespace objects and triggers OnChange when the set
// of namespaces that should be watched diverges from the set resolved at
// startup. The operator uses a namespace-scoped cache whose scope is fixed at
// build time, so the only way to apply a changed set is to restart: OnChange
// cancels the manager context, the process exits cleanly, and Kubernetes starts
// a new pod that resolves the set again.
type NamespaceWatcher struct {
	client.Client
	// Filter decides whether a namespace should be watched.
	Filter *NamespaceFilter
	// Watched is the set of namespace names resolved at startup (the current
	// cache scope), used as the reference to detect divergence.
	Watched map[string]struct{}
	// OnChange is invoked when a divergence is detected (it cancels the manager
	// context). It must be idempotent; a context cancel func is.
	OnChange func()
}

// Reconcile compares whether the reconciled namespace should currently be
// watched against whether it was watched at startup. On any mismatch the
// watched set has changed, so it triggers OnChange to restart the operator.
func (w *NamespaceWatcher) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logr.FromContext(ctx)

	should := false
	ns := &corev1.Namespace{}
	switch err := w.Get(ctx, req.NamespacedName, ns); {
	case apierrors.IsNotFound(err):
		// Deleted namespace: it should no longer be watched.
		should = false
	case err != nil:
		return ctrl.Result{}, err
	default:
		should = w.Filter.Matches(ns.Name, ns.Labels)
	}

	if _, was := w.Watched[req.Name]; should != was {
		log.Info("watched namespace set changed, restarting operator to apply new cache scope",
			"namespace", req.Name, "shouldWatch", should, "wasWatched", was)
		w.OnChange()
	}

	return ctrl.Result{}, nil
}

// SetupWithManager registers the NamespaceWatcher with the manager.
func (w *NamespaceWatcher) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Namespace{}).
		Complete(w)
}
