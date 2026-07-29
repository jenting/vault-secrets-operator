package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	return s
}

func TestNamespaceFilter(t *testing.T) {
	tests := []struct {
		name          string
		namespaces    []string
		labelSelector string
		wantErr       bool
		enabled       bool
		usesSelector  bool
		matchName     string
		matchLabels   map[string]string
		wantMatch     bool
	}{
		{
			name:      "disabled matches everything",
			enabled:   false,
			matchName: "anything",
			wantMatch: true,
		},
		{
			name:       "explicit name in list matches",
			namespaces: []string{"team-a", "team-b"},
			enabled:    true,
			matchName:  "team-a",
			wantMatch:  true,
		},
		{
			name:       "explicit name not in list does not match",
			namespaces: []string{"team-a"},
			enabled:    true,
			matchName:  "team-c",
			wantMatch:  false,
		},
		{
			name:          "label selector matches",
			labelSelector: "watch=true",
			enabled:       true,
			usesSelector:  true,
			matchName:     "team-x",
			matchLabels:   map[string]string{"watch": "true"},
			wantMatch:     true,
		},
		{
			name:          "label selector does not match",
			labelSelector: "watch=true",
			enabled:       true,
			usesSelector:  true,
			matchName:     "team-x",
			matchLabels:   map[string]string{"watch": "false"},
			wantMatch:     false,
		},
		{
			name:          "OR union: name matches even when label does not",
			namespaces:    []string{"team-a"},
			labelSelector: "watch=true",
			enabled:       true,
			usesSelector:  true,
			matchName:     "team-a",
			matchLabels:   map[string]string{"watch": "false"},
			wantMatch:     true,
		},
		{
			name:          "OR union: label matches even when name is absent",
			namespaces:    []string{"team-a"},
			labelSelector: "watch=true",
			enabled:       true,
			usesSelector:  true,
			matchName:     "team-z",
			matchLabels:   map[string]string{"watch": "true"},
			wantMatch:     true,
		},
		{
			name:          "invalid selector errors",
			labelSelector: "!!!bad",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := NewNamespaceFilter(tt.namespaces, tt.labelSelector)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := f.Enabled(); got != tt.enabled {
				t.Errorf("Enabled() = %v, want %v", got, tt.enabled)
			}
			if got := f.UsesLabelSelector(); got != tt.usesSelector {
				t.Errorf("UsesLabelSelector() = %v, want %v", got, tt.usesSelector)
			}
			if got := f.Matches(tt.matchName, tt.matchLabels); got != tt.wantMatch {
				t.Errorf("Matches(%q, %v) = %v, want %v", tt.matchName, tt.matchLabels, got, tt.wantMatch)
			}
		})
	}
}

func TestWatchedNamespaces(t *testing.T) {
	scheme := testScheme(t)
	nsLabeled := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "labeled", Labels: map[string]string{"watch": "true"}},
	}
	nsOther := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "other"}}
	nsExplicit := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "explicit"}}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nsLabeled, nsOther, nsExplicit).Build()

	// Union of explicit name "explicit" and label-matched "labeled".
	filter, err := NewNamespaceFilter([]string{"explicit"}, "watch=true")
	if err != nil {
		t.Fatalf("filter: %v", err)
	}
	set, err := filter.WatchedNamespaces(context.Background(), cl)
	if err != nil {
		t.Fatalf("WatchedNamespaces: %v", err)
	}
	if len(set) != 2 {
		t.Fatalf("expected 2 namespaces, got %v", set)
	}
	if _, ok := set["labeled"]; !ok {
		t.Errorf("expected label-matched namespace 'labeled' in set")
	}
	if _, ok := set["explicit"]; !ok {
		t.Errorf("expected explicit namespace 'explicit' in set")
	}
	if _, ok := set["other"]; ok {
		t.Errorf("did not expect non-matching namespace 'other' in set")
	}
}

func TestNamespaceWatcher(t *testing.T) {
	scheme := testScheme(t)
	filter, err := NewNamespaceFilter(nil, "watch=true")
	if err != nil {
		t.Fatalf("filter: %v", err)
	}

	labeled := func(name string) *corev1.Namespace {
		return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{"watch": "true"}}}
	}
	unlabeled := func(name string) *corev1.Namespace {
		return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	}

	tests := []struct {
		name       string
		objects    []*corev1.Namespace
		watched    map[string]struct{}
		reconcile  string
		wantChange bool
	}{
		{
			name:       "new matching namespace triggers restart",
			objects:    []*corev1.Namespace{labeled("team-new")},
			watched:    map[string]struct{}{},
			reconcile:  "team-new",
			wantChange: true,
		},
		{
			name:       "namespace lost label triggers restart",
			objects:    []*corev1.Namespace{unlabeled("team-a")},
			watched:    map[string]struct{}{"team-a": {}},
			reconcile:  "team-a",
			wantChange: true,
		},
		{
			name:       "deleted watched namespace triggers restart",
			objects:    nil, // team-a does not exist -> NotFound
			watched:    map[string]struct{}{"team-a": {}},
			reconcile:  "team-a",
			wantChange: true,
		},
		{
			name:       "still-matching namespace does not restart",
			objects:    []*corev1.Namespace{labeled("team-a")},
			watched:    map[string]struct{}{"team-a": {}},
			reconcile:  "team-a",
			wantChange: false,
		},
		{
			name:       "unrelated non-matching namespace does not restart",
			objects:    []*corev1.Namespace{unlabeled("other")},
			watched:    map[string]struct{}{},
			reconcile:  "other",
			wantChange: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			for _, o := range tt.objects {
				builder = builder.WithObjects(o)
			}
			changed := false
			w := &NamespaceWatcher{
				Client:   builder.Build(),
				Filter:   filter,
				Watched:  tt.watched,
				OnChange: func() { changed = true },
			}
			if _, err := w.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{Name: tt.reconcile},
			}); err != nil {
				t.Fatalf("Reconcile(%q): %v", tt.reconcile, err)
			}
			if changed != tt.wantChange {
				t.Errorf("OnChange fired = %v, want %v", changed, tt.wantChange)
			}
		})
	}
}
