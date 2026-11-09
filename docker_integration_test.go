//go:build integration

package traefikkop

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/client"
	"github.com/stretchr/testify/require"
	"github.com/traefik/traefik/v3/pkg/config/dynamic"
	"github.com/traefik/traefik/v3/pkg/provider/docker"
)

func TestMobyClientWithRedisAndTraefikContainers(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("Docker CLI is required for integration tests")
	}

	dockerSocket := os.Getenv("TRAEFIK_KOP_DOCKER_SOCKET")
	dockerHost := os.Getenv("DOCKER_HOST")
	if dockerHost == "" {
		output, err := exec.Command("docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}").CombinedOutput()
		require.NoError(t, err, "inspect active Docker context: %s", output)
		dockerHost = strings.TrimSpace(string(output))
		if dockerHost == "" {
			dockerHost = "unix:///var/run/docker.sock"
		}
	}
	if dockerSocket == "" {
		u, err := url.Parse(dockerHost)
		require.NoError(t, err)
		if u.Scheme != "unix" {
			t.Skip("set TRAEFIK_KOP_DOCKER_SOCKET to a local Docker socket when using a non-Unix DOCKER_HOST")
		}
		dockerSocket = u.Path
	}
	if _, err := os.Stat(dockerSocket); err != nil {
		t.Skipf("Docker socket %q is unavailable: %v", dockerSocket, err)
	}

	_, currentFile, _, ok := runtime.Caller(0)
	require.True(t, ok)
	composeFile := filepath.Join(filepath.Dir(currentFile), "testing", "integration", "docker-compose.yml")
	project := fmt.Sprintf("traefik-kop-it-%d", time.Now().UnixNano())
	compose := func(args ...string) string {
		t.Helper()
		baseArgs := []string{"compose", "-f", composeFile, "-p", project}
		cmd := exec.CommandContext(t.Context(), "docker", append(baseArgs, args...)...)
		cmd.Env = append(os.Environ(), "TRAEFIK_KOP_DOCKER_SOCKET="+dockerSocket)
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "docker compose %s failed:\n%s", strings.Join(args, " "), output)
		return strings.TrimSpace(string(output))
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, "docker", "compose", "-f", composeFile, "-p", project, "down", "--volumes", "--remove-orphans")
		cmd.Env = append(os.Environ(), "TRAEFIK_KOP_DOCKER_SOCKET="+dockerSocket)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Errorf("docker compose down failed: %v\n%s", err, output)
		}
	})

	compose("up", "--detach", "--wait", "--wait-timeout", "180")
	redisAddr := composePort(t, compose, "redis", "6379")
	traefikAddr := composePort(t, compose, "traefik", "80")

	dockerClient, err := createDockerClient(dockerHost)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dockerClient.Close()) })

	proxyApp, proxyEndpoint := createProxy(dockerClient, "").start()
	t.Cleanup(func() { require.NoError(t, proxyApp.Shutdown()) })
	proxiedClient, err := createDockerClient(proxyEndpoint)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, proxiedClient.Close()) })

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	require.Eventually(t, func() bool {
		containers, err := proxiedClient.ContainerList(ctx, client.ContainerListOptions{})
		if err != nil {
			return false
		}
		for _, container := range containers.Items {
			if container.Labels["traefik.http.services.integration.loadbalancer.server.port"] == "80" {
				inspect, err := proxiedClient.ContainerInspect(ctx, container.ID, client.ContainerInspectOptions{})
				return err == nil && inspect.Container.Config != nil &&
					inspect.Container.Config.Labels["traefik.http.routers.integration.rule"] == "Host(`integration.test`)"
			}
		}
		return false
	}, 30*time.Second, 500*time.Millisecond, "Moby client should list and inspect the labeled whoami container")

	store := NewRedisStore("integration", redisAddr, 0, "", "", 0, nil, "", false)
	redisStore, ok := store.(*RedisStore)
	require.True(t, ok)
	t.Cleanup(func() { require.NoError(t, redisStore.client.Close()) })
	require.Eventually(t, func() bool { return store.Ping() == nil }, 30*time.Second, 500*time.Millisecond, "Redis should be reachable")

	var configuration dynamic.Configuration
	rawConfiguration := fmt.Sprintf(`{
		"http": {
			"routers": {
				"integration": {
					"entryPoints": ["web"],
					"rule": %q,
					"service": "integration"
				}
			},
			"services": {
				"integration": {
					"loadBalancer": {
						"servers": [{"url": "http://whoami:80"}]
					}
				}
			}
		}
	}`, "Host(`integration.test`)")
	require.NoError(t, json.Unmarshal([]byte(rawConfiguration), &configuration))
	configuration.TCP = &dynamic.TCPConfiguration{}
	configuration.UDP = &dynamic.UDPConfiguration{}

	provider := &docker.Provider{UseBindPortIP: true}
	config := Config{DockerHost: dockerHost, SkipReplace: true}
	createConfigHandler(config, store, provider, proxiedClient)(configuration)

	serverKey := "traefik/http/services/integration/loadBalancer/servers/0/url"
	require.Eventually(t, func() bool {
		value, err := store.Get(serverKey)
		return err == nil && value == "http://whoami:80"
	}, 10*time.Second, 200*time.Millisecond, "the config handler should write the backend URL to Redis")

	client := &http.Client{Timeout: 2 * time.Second}
	require.Eventually(t, func() bool {
		request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+traefikAddr, nil)
		if err != nil {
			return false
		}
		request.Host = "integration.test"
		response, err := client.Do(request)
		if err != nil {
			return false
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		return err == nil && response.StatusCode == http.StatusOK && strings.Contains(string(body), "Hostname: integration-whoami")
	}, 60*time.Second, 500*time.Millisecond, "Traefik should load the Redis configuration and route to the whoami container")
}

func composePort(t *testing.T, compose func(...string) string, service, containerPort string) string {
	t.Helper()

	output := compose("port", service, containerPort)
	line := strings.TrimSpace(strings.Split(output, "\n")[0])
	_, port, err := net.SplitHostPort(line)
	require.NoError(t, err, "unexpected docker compose port output %q", output)
	return net.JoinHostPort("127.0.0.1", port)
}
