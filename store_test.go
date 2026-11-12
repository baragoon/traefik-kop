package traefikkop

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/alicebob/miniredis/v2"
	"github.com/moby/moby/api/types/container"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/traefik/traefik/v3/pkg/config/dynamic"
)

const NGINX_CONF_JSON = `{"http":{"routers":{"nginx@docker":{"service":"nginx","rule":"Host('nginx.local')"}},"services":{"nginx@docker":{"loadBalancer":{"servers":[{"url":"http://172.20.0.2:80"}],"passHostHeader":true}}}},"tcp":{},"udp":{},"tls":{"options":{"default":{"clientAuth":{},"alpnProtocols":["h2","http/1.1","acme-tls/1"]}}}}`
const NGINX_CONF_JSON_DIFFRENT_SERVICE_NAME = `{"http":{"routers":{"nginx@docker":{"service":"nginx-nginx","rule":"Host('nginx.local')"}},"services":{"nginx-nginx@docker":{"loadBalancer":{"servers":[{"url":"http://172.20.0.2:80"}],"passHostHeader":true}}}},"tcp":{},"udp":{},"tls":{"options":{"default":{"clientAuth":{},"alpnProtocols":["h2","http/1.1","acme-tls/1"]}}}}`

func Test_collectKeys(t *testing.T) {
	cfg := &dynamic.Configuration{}
	_, err := toml.DecodeFile("./fixtures/sample.toml", &cfg)
	require.NoError(t, err)

	keys := collectKeys(cfg.HTTP.Middlewares)
	require.NotEmpty(t, keys)
	require.Contains(t, keys, "Middleware21")

	require.Contains(t, collectKeys(cfg.HTTP.Services), "Service0")

	cfg = &dynamic.Configuration{}
	err = json.Unmarshal([]byte(NGINX_CONF_JSON), cfg)
	require.NoError(t, err)
	keys = collectKeys(cfg.HTTP.Routers)
	require.Len(t, keys, 1)
}

// keys := collectKeys(cfg.HTTP.Middlewares)
// require.NotEmpty(t, keys)
// require.True(t, keys.Contains("Middleware21"))

// require.True(t, collectKeys(cfg.HTTP.Services).Contains("Service0"))

func Test_redisStore(t *testing.T) {
	server, err := miniredis.Run()
	require.NoError(t, err)
	defer server.Close()

	store := NewRedisStore("localhost", server.Addr(), 0, "", "", 0, nil, "", false)
	processFileWithConfig(t, store, nil, "hellodetect.yml")
	assertServiceIPs(t, store, []svc{
		{"hello-detect", "http", "http://192.168.100.100:5577"},
		{"hello-detect2", "http", "http://192.168.100.100:5577"},
	})
}

func Test_redisStoreKeepConfAliveRefreshesTTL(t *testing.T) {
	server, err := miniredis.Run()
	require.NoError(t, err)
	defer server.Close()

	store := NewRedisStore("localhost", server.Addr(), 30, "", "", 0, nil, "", false)
	rs := store.(*RedisStore)

	conf := &dynamic.Configuration{}
	require.NoError(t, json.Unmarshal([]byte(NGINX_CONF_JSON), conf))
	require.NoError(t, store.Store(*conf))

	server.FastForward(20 * time.Second)
	require.NoError(t, store.KeepConfAlive())

	serviceKey := "traefik/http/services/nginx/loadBalancer/servers/0/url"
	require.Equal(t, 30*time.Second, server.TTL(serviceKey))
	require.Equal(t, 30*time.Second, server.TTL(rs.sk("kop_last_update")))

	server.Del(serviceKey)
	require.NoError(t, store.KeepConfAlive())

	value, err := store.Get(serviceKey)
	require.NoError(t, err)
	require.Equal(t, "http://172.20.0.2:80", value)
	require.Equal(t, 30*time.Second, server.TTL(serviceKey))
}

type blockingPipelineHook struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (h *blockingPipelineHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *blockingPipelineHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return next
}

func (h *blockingPipelineHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		err := next(ctx, cmds)
		h.once.Do(func() {
			close(h.started)
			<-h.release
		})
		return err
	}
}

func Test_redisStoreKeepConfAliveDoesNotRestoreStaleConfig(t *testing.T) {
	server, err := miniredis.Run()
	require.NoError(t, err)
	defer server.Close()

	store := NewRedisStore("localhost", server.Addr(), 30, "", "", 0, nil, "", false)
	rs := store.(*RedisStore)

	oldConf := &dynamic.Configuration{}
	require.NoError(t, json.Unmarshal([]byte(NGINX_CONF_JSON), oldConf))
	require.NoError(t, store.Store(*oldConf))
	server.FastForward(30 * time.Second)

	hook := &blockingPipelineHook{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	rs.client.AddHook(hook)
	var releaseOnce sync.Once
	releasePipeline := func() {
		releaseOnce.Do(func() {
			close(hook.release)
		})
	}
	defer releasePipeline()

	keepAliveDone := make(chan error, 1)
	go func() {
		keepAliveDone <- store.KeepConfAlive()
	}()
	select {
	case <-hook.started:
	case <-time.After(time.Second):
		t.Fatal("keepalive did not execute its TTL refresh pipeline")
	}

	newConf := &dynamic.Configuration{}
	require.NoError(t, json.Unmarshal([]byte(NGINX_CONF_JSON_DIFFRENT_SERVICE_NAME), newConf))
	storeAttempted := make(chan struct{})
	rs.onStoreAttempt = func() {
		close(storeAttempted)
	}
	storeDone := make(chan error, 1)
	go func() {
		storeDone <- store.Store(*newConf)
	}()
	select {
	case <-storeAttempted:
	case <-time.After(time.Second):
		t.Fatal("new configuration store was not attempted")
	}

	select {
	case err := <-storeDone:
		require.NoError(t, err)
		t.Fatal("new configuration store completed while keepalive recovery was in progress")
	case <-time.After(50 * time.Millisecond):
	}

	releasePipeline()
	require.NoError(t, <-keepAliveDone)
	require.NoError(t, <-storeDone)

	newServiceKey := "traefik/http/services/nginx-nginx/loadBalancer/servers/0/url"
	value, err := store.Get(newServiceKey)
	require.NoError(t, err)
	require.Equal(t, "http://172.20.0.2:80", value)

	oldServiceKey := "traefik/http/services/nginx/loadBalancer/servers/0/url"
	value, err = store.Get(oldServiceKey)
	require.NoError(t, err)
	require.Empty(t, value)
}

func Test_redisStoreTLSOpts(t *testing.T) {
	store := NewRedisStore("redis.domain", "redis.domain:443", 0, "", "", 0, nil, "", true)
	rs, ok := store.(*RedisStore)
	require.True(t, ok)
	require.NotNil(t, rs.client.Options().TLSConfig)
	assert.Equal(t, "redis.domain", rs.client.Options().TLSConfig.ServerName)
	assert.Equal(t, uint16(tls.VersionTLS12), rs.client.Options().TLSConfig.MinVersion)

	overrideStore := NewRedisStore("redis.domain", "192.168.90.3:6379", 0, "", "", 0, nil, "", true, "redis.example.com")
	overrideRS, ok := overrideStore.(*RedisStore)
	require.True(t, ok)
	require.NotNil(t, overrideRS.client.Options().TLSConfig)
	assert.Equal(t, "redis.example.com", overrideRS.client.Options().TLSConfig.ServerName)

	plainStore := NewRedisStore("localhost", "localhost:6379", 0, "", "", 0, nil, "", false)
	plainRS, ok := plainStore.(*RedisStore)
	require.True(t, ok)
	assert.Nil(t, plainRS.client.Options().TLSConfig)

	directConfig := redisTLSConfigForAddr("redis.domain:443")
	require.NotNil(t, directConfig)
	assert.Equal(t, "redis.domain", directConfig.ServerName)
	assert.Equal(t, uint16(tls.VersionTLS12), directConfig.MinVersion)

	overrideConfig := redisTLSConfigForAddr("192.168.90.3:6379", "redis.example.com")
	require.NotNil(t, overrideConfig)
	assert.Equal(t, "redis.example.com", overrideConfig.ServerName)

	sentinelConfig := redisTLSConfigForAddr("sentinel.domain:26379")
	require.NotNil(t, sentinelConfig)
	assert.Equal(t, "sentinel.domain", sentinelConfig.ServerName)
	assert.NotEqual(t, directConfig.ServerName, sentinelConfig.ServerName)
}

func Test_redisStoreRemovesStaleServerKeysOnUpdate(t *testing.T) {
	server, err := miniredis.Run()
	require.NoError(t, err)
	defer server.Close()

	store := NewRedisStore("localhost", server.Addr(), 0, "", "", 0, nil, "", false)

	initial := &dynamic.Configuration{}
	err = json.Unmarshal([]byte(`{
		"http": {
			"services": {
				"app@docker": {
					"loadBalancer": {
						"servers": [
							{"url": "http://10.0.0.1:8080"},
							{"url": "http://10.0.0.2:8080"}
						]
					}
				}
			}
		},
		"tcp": {},
		"udp": {}
	}`), initial)
	require.NoError(t, err)
	require.NoError(t, store.Store(*initial))

	staleKey := "traefik/http/services/app/loadBalancer/servers/1/url"
	val, err := store.Get(staleKey)
	require.NoError(t, err)
	require.Equal(t, "http://10.0.0.2:8080", val)

	updated := &dynamic.Configuration{}
	err = json.Unmarshal([]byte(`{
		"http": {
			"services": {
				"app@docker": {
					"loadBalancer": {
						"servers": [
							{"url": "http://10.0.0.3:8080"}
						]
					}
				}
			}
		},
		"tcp": {},
		"udp": {}
	}`), updated)
	require.NoError(t, err)
	require.NoError(t, store.Store(*updated))

	remaining, err := store.Get("traefik/http/services/app/loadBalancer/servers/0/url")
	require.NoError(t, err)
	require.Equal(t, "http://10.0.0.3:8080", remaining)

	stale, err := store.Get(staleKey)
	require.NoError(t, err)
	require.Empty(t, stale)
}

func Test_redisStoreKeepConfAliveDoesNotRenewPeerServers(t *testing.T) {
	for _, proto := range []string{"http", "tcp", "udp"} {
		t.Run(proto, func(t *testing.T) {
			server := miniredis.RunT(t)
			store := NewRedisStore("localhost", server.Addr(), 30, "", "", 0, nil, "", false)
			rs := store.(*RedisStore)
			field, local, peer := "address", "127.0.0.1:80", "127.0.0.2:80"
			if proto == "http" {
				field, local, peer = "url", "http://127.0.0.1:80", "http://127.0.0.2:80"
			}
			conf := &dynamic.Configuration{}
			require.NoError(t, json.Unmarshal([]byte(fmt.Sprintf(`{"http":{},"tcp":{},"udp":{},"%s":{"services":{"nginx@docker":{"loadBalancer":{"servers":[{"%s":"%s"}]}}}}}`, proto, field, local)), conf))
			prefix := fmt.Sprintf("traefik/%s/services/nginx/loadBalancer/servers/", proto)
			require.NoError(t, server.Set(prefix+"0/"+field, peer))
			server.SetTTL(prefix+"0/"+field, 30*time.Second)
			dc := &dockerCache{client: createTestClient(map[string]string{
				"kop.merge-lbs": "true",
				"traefik." + proto + ".services.nginx.loadbalancer.server.port": "80",
			}), details: make(map[string]container.InspectResponse)}
			mergeLoadBalancers(dc, conf, store)
			require.NoError(t, store.Store(*conf))
			require.NoError(t, store.Store(*conf)) // Reusing the same merged configuration retains ownership.
			kv, err := store.Gets(prefix + "*")
			require.NoError(t, err)
			require.Len(t, kv, 2)
			var localKey, peerKey string
			for key, value := range kv {
				if value == local {
					localKey = key
				} else {
					peerKey = key
				}
			}
			require.NotEmpty(t, localKey)
			require.NotEmpty(t, peerKey)
			server.FastForward(20 * time.Second)
			require.NoError(t, store.KeepConfAlive())
			require.Equal(t, 30*time.Second, server.TTL(localKey))
			require.Equal(t, 10*time.Second, server.TTL(peerKey))
			server.FastForward(11 * time.Second)
			require.NoError(t, store.KeepConfAlive())
			require.False(t, server.Exists(peerKey))
			server.Del(localKey)
			require.NoError(t, store.KeepConfAlive())
			require.True(t, server.Exists(localKey))
			require.False(t, server.Exists(peerKey))
			server.FlushAll()
			require.NoError(t, store.KeepConfAlive())
			require.True(t, server.Exists(localKey))
			require.False(t, server.Exists(peerKey))
			require.True(t, server.Exists(rs.sk("kop_last_update")))
			require.True(t, server.Exists(rs.sk(proto+"_services")))
		})
	}
}
