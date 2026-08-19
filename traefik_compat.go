package traefikkop

import (
	"github.com/traefik/traefik/v3/pkg/provider"
	"github.com/traefik/traefik/v3/pkg/safe"
	"github.com/traefik/traefik/v3/pkg/server"
)

func newConfigurationWatcher(
	routinesPool *safe.Pool,
	pvd provider.Provider,
	defaultEntryPoints []string,
	requiredProvider string,
) *server.ConfigurationWatcher {
	return server.NewConfigurationWatcher(routinesPool, pvd, defaultEntryPoints, requiredProvider, false)
}
