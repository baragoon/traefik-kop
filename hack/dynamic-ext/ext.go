// Package ext is a local stub mirroring github.com/traefik/traefik/dynamic/ext,
// referenced via a replace directive in go.mod. Traefik's own go.mod uses a
// path-based replace for this sub-module, which is unresolvable by downstream
// consumers. See https://github.com/traefik/traefik/issues/13115.
package ext

// HTTP is a dynamic.HTTP extension.
type HTTP struct{}

// Router is a dynamic.Router extension.
type Router struct{}
