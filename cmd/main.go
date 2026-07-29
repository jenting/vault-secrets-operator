package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"

	ricobergerdev1alpha1 "github.com/ricoberger/vault-secrets-operator/api/v1alpha1"
	"github.com/ricoberger/vault-secrets-operator/internal/controller"
	"github.com/ricoberger/vault-secrets-operator/internal/vault"

	// +kubebuilder:scaffold:imports

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	webhookserver "sigs.k8s.io/controller-runtime/pkg/webhook"
)

// noMatchingNamespace is a placeholder namespace name used to keep the cache
// namespace-scoped when the configuration currently matches no namespace.
// controller-runtime treats an empty cache.DefaultNamespaces map as
// cluster-wide (it only builds a scoped cache when len > 0), so without a
// placeholder a selector matching nothing would accidentally watch every
// namespace. This name is very unlikely to exist; the NamespaceWatcher restarts
// the operator as soon as a real namespace matches.
const noMatchingNamespace = "vault-secrets-operator-no-matching-namespace"

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(ricobergerdev1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

func main() {
	var metricsAddr string
	var probeAddr string
	var enableLeaderElection bool
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false, "Enable leader election for controller manager. Enabling this will ensure there is only one active controller manager.")
	opts := zap.Options{
		Development: false,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// Create the API client for Vault and start the renew process for the
	// token in a new goroutine.
	err := vault.InitSharedClient()
	if err != nil {
		ctrl.Log.Error(err, "Could not create API client for Vault")
		os.Exit(1)
	}

	if vault.SharedClient != nil {
		if vault.SharedClient.PerformRenewToken() {
			go vault.SharedClient.RenewToken()
		}
	} else {
		ctrl.Log.Info("Shared client wasn't initialized, each secret must be use the vaultRole property")
	}

	watchNamespace, err := getWatchNamespace()
	if err != nil {
		setupLog.Error(err, "unable to get WatchNamespace, the manager will watch and manage resources in all namespaces")
	}

	labelSelector := os.Getenv("WATCH_NAMESPACE_LABEL_SELECTOR")

	// Normalize the comma-separated namespace list (e.g. "ns1, ns2").
	space := regexp.MustCompile(`\s+`)
	var namespaceList []string
	if watchNamespace != "" {
		for _, ns := range strings.Split(space.ReplaceAllString(watchNamespace, ""), ",") {
			if ns != "" {
				namespaceList = append(namespaceList, ns)
			}
		}
	}

	nsFilter, err := controller.NewNamespaceFilter(namespaceList, labelSelector)
	if err != nil {
		setupLog.Error(err, "invalid WATCH_NAMESPACE_LABEL_SELECTOR")
		os.Exit(1)
	}

	restConfig := ctrl.GetConfigOrDie()

	options := ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: metricsAddr,
		},
		WebhookServer: webhookserver.NewServer(webhookserver.Options{
			Port: 9443,
		}),
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "vaultsecretsoperator.ricoberger.de",
	}

	// Scope the cache to the namespaces to watch (keeps it namespace-scoped for
	// low memory). watchedNamespaces also seeds the NamespaceWatcher below.
	watchedNamespaces, err := scopeCacheToNamespaces(restConfig, nsFilter, &options)
	if err != nil {
		setupLog.Error(err, "unable to scope cache to watched namespaces")
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(restConfig, options)
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	if err = (&controller.VaultSecretReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "VaultSecret")
		os.Exit(1)
	}
	// +kubebuilder:scaffold:builder

	err = mgr.AddHealthzCheck("healthz", func(_ *http.Request) error {
		if vault.SharedClient == nil {
			return nil
		}

		return vault.SharedClient.GetHealth(10)
	})
	if err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}

	err = mgr.AddReadyzCheck("readyz", func(_ *http.Request) error {
		if vault.SharedClient == nil {
			return nil
		}

		return vault.SharedClient.GetHealth(5)
	})
	if err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	// A cancelable context so the NamespaceWatcher can trigger a graceful
	// shutdown (and exit) when the watched namespace set changes.
	ctx, cancel := context.WithCancel(ctrl.SetupSignalHandler())
	defer cancel()

	if nsFilter.UsesLabelSelector() {
		if err = (&controller.NamespaceWatcher{
			Client:   mgr.GetClient(),
			Filter:   nsFilter,
			Watched:  watchedNamespaces,
			OnChange: cancel,
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "NamespaceWatcher")
			os.Exit(1)
		}
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctx); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

// scopeCacheToNamespaces resolves the namespaces to watch (explicit
// WATCH_NAMESPACE names unioned with label-matched namespaces) and scopes the
// manager cache to them via DefaultNamespaces, keeping the cache
// namespace-scoped for low memory. It returns the resolved set, which seeds the
// NamespaceWatcher. When nothing matches yet it scopes to noMatchingNamespace.
func scopeCacheToNamespaces(restConfig *rest.Config, filter *controller.NamespaceFilter, options *ctrl.Options) (map[string]struct{}, error) {
	if !filter.Enabled() {
		return nil, nil
	}

	directClient, err := client.New(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		return nil, err
	}
	watched, err := filter.WatchedNamespaces(context.Background(), directClient)
	if err != nil {
		return nil, err
	}

	namespaces := make(map[string]cache.Config, len(watched))
	for ns := range watched {
		namespaces[ns] = cache.Config{}
	}
	if len(namespaces) == 0 {
		setupLog.Info("no namespace matches the configuration yet, watching nothing until one appears")
		namespaces[noMatchingNamespace] = cache.Config{}
	} else {
		setupLog.Info("manager set up with namespace-scoped cache", "namespaceCount", len(namespaces))
	}

	options.NewCache = func(config *rest.Config, opts cache.Options) (cache.Cache, error) {
		opts.DefaultNamespaces = namespaces
		return cache.New(config, opts)
	}
	return watched, nil
}

// getWatchNamespace returns the Namespace the operator should be watching for
// changes
func getWatchNamespace() (string, error) {
	// WatchNamespaceEnvVar is the constant for env variable WATCH_NAMESPACE
	// which specifies the Namespace to watch. An empty value means the operator
	// is running with cluster scope.
	var watchNamespaceEnvVar = "WATCH_NAMESPACE"

	ns, found := os.LookupEnv(watchNamespaceEnvVar)
	if !found {
		return "", fmt.Errorf("%s must be set", watchNamespaceEnvVar)
	}
	return ns, nil
}
