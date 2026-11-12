package traefikkop

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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

type failingSetHook struct {
	key  string
	fail bool
}

func (h *failingSetHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *failingSetHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		args := cmd.Args()
		if h.fail && len(args) > 1 && args[0] == "set" {
			key, ok := args[1].(string)
			if ok && (key == h.key || strings.Contains(key, ":config:")) {
				return errors.New("injected Redis write failure")
			}
		}
		return next(ctx, cmd)
	}
}

func (h *failingSetHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
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

func Test_redisStoreKeepConfAliveRestoresOnlyMissingKeys(t *testing.T) {
	server := miniredis.RunT(t)
	store := NewRedisStore("localhost", server.Addr(), 30, "", "", 0, nil, "", false)
	rs := store.(*RedisStore)

	conf := &dynamic.Configuration{}
	require.NoError(t, json.Unmarshal([]byte(NGINX_CONF_JSON), conf))
	require.NoError(t, store.Store(*conf))

	serviceKey := "traefik/http/services/nginx/loadBalancer/servers/0/url"
	ruleKey := "traefik/http/routers/nginx/rule"
	require.NoError(t, server.Set(ruleKey, "Host(`newer.local`)"))
	server.Del(serviceKey)

	require.NoError(t, store.KeepConfAlive())

	value, err := store.Get(serviceKey)
	require.NoError(t, err)
	require.Equal(t, "http://172.20.0.2:80", value)
	value, err = store.Get(ruleKey)
	require.NoError(t, err)
	require.Equal(t, "Host(`newer.local`)", value)
	require.True(t, server.Exists(rs.sk("kop_last_update")))
}

func Test_redisStoreKeepConfAliveDoesNotRestoreRemovedRoutes(t *testing.T) {
	server := miniredis.RunT(t)
	store := NewRedisStore("localhost", server.Addr(), 30, "", "", 0, nil, "", false)
	rs := store.(*RedisStore)

	conf := &dynamic.Configuration{}
	require.NoError(t, json.Unmarshal([]byte(NGINX_CONF_JSON), conf))
	require.NoError(t, store.Store(*conf))

	routerPrefix := "traefik/http/routers/nginx/"
	routerKeys, err := rs.client.Keys(context.Background(), routerPrefix+"*").Result()
	require.NoError(t, err)
	require.NotEmpty(t, routerKeys)
	conf.HTTP.Routers = make(map[string]*dynamic.Router)
	require.NoError(t, store.Store(*conf))

	require.NoError(t, store.KeepConfAlive())
	for _, key := range routerKeys {
		require.False(t, server.Exists(key), "removed route key %s was restored", key)
	}
	require.False(t, server.Exists(rs.sk("http_routers")))
}

func Test_redisStoreKeepConfAliveChecksKeysWithoutTTL(t *testing.T) {
	server := miniredis.RunT(t)
	store := NewRedisStore("localhost", server.Addr(), 0, "", "", 0, nil, "", false)
	rs := store.(*RedisStore)

	conf := &dynamic.Configuration{}
	require.NoError(t, json.Unmarshal([]byte(NGINX_CONF_JSON), conf))
	require.NoError(t, store.Store(*conf))

	serviceKey := "traefik/http/services/nginx/loadBalancer/servers/0/url"
	server.Del(serviceKey)
	require.True(t, server.Exists("traefik_kop_last_update@localhost"))

	require.NoError(t, store.KeepConfAlive())
	value, err := store.Get(serviceKey)
	require.NoError(t, err)
	require.Equal(t, "http://172.20.0.2:80", value)

	server.Del(rs.sk("http_services"))
	require.NoError(t, store.KeepConfAlive())
	members, err := server.SMembers(rs.sk("http_services"))
	require.NoError(t, err)
	require.Equal(t, []string{"nginx@docker"}, members)

	emptyConf := &dynamic.Configuration{}
	require.NoError(t, json.Unmarshal([]byte(`{"http":{},"tcp":{},"udp":{}}`), emptyConf))
	require.NoError(t, store.Store(*emptyConf))
	value, err = store.Get(serviceKey)
	require.NoError(t, err)
	require.Empty(t, value)
}

func Test_redisStoreStoreWriteFailuresDoNotAdvanceCache(t *testing.T) {
	server := miniredis.RunT(t)
	store := NewRedisStore("localhost", server.Addr(), 30, "", "", 0, nil, "", false)
	rs := store.(*RedisStore)

	oldConf := &dynamic.Configuration{}
	require.NoError(t, json.Unmarshal([]byte(NGINX_CONF_JSON), oldConf))
	require.NoError(t, store.Store(*oldConf))

	serviceKey := "traefik/http/services/nginx/loadBalancer/servers/0/url"
	hook := &failingSetHook{key: serviceKey, fail: true}
	rs.client.AddHook(hook)

	updatedJSON := strings.Replace(NGINX_CONF_JSON, "http://172.20.0.2:80", "http://172.20.0.3:80", 1)
	updatedConf := &dynamic.Configuration{}
	require.NoError(t, json.Unmarshal([]byte(updatedJSON), updatedConf))
	require.Error(t, store.Store(*updatedConf))

	value, err := store.Get(serviceKey)
	require.NoError(t, err)
	require.Equal(t, "http://172.20.0.2:80", value)
	require.Equal(t, oldConf.HTTP.Services["nginx@docker"].LoadBalancer.Servers[0].URL,
		rs.lastConfig.HTTP.Services["nginx@docker"].LoadBalancer.Servers[0].URL)
	require.Equal(t, "http://172.20.0.2:80", rs.localKV[serviceKey])

	server.Del(serviceKey)
	require.Error(t, store.KeepConfAlive())
	hook.fail = false
	require.NoError(t, store.KeepConfAlive())

	server.Set(serviceKey, "stale-value")
	require.NoError(t, store.KeepConfAlive())
	value, err = store.Get(serviceKey)
	require.NoError(t, err)
	require.Equal(t, "stale-value", value)
}

func Test_redisStoreRemovesExpiredResourcesWithoutEmptyUnlink(t *testing.T) {
	server := miniredis.RunT(t)
	store := NewRedisStore("localhost", server.Addr(), 10, "", "", 0, nil, "", false)

	conf := &dynamic.Configuration{}
	require.NoError(t, json.Unmarshal([]byte(NGINX_CONF_JSON), conf))
	require.NoError(t, store.Store(*conf))
	server.FastForward(10 * time.Second)

	emptyConf := &dynamic.Configuration{}
	require.NoError(t, json.Unmarshal([]byte(`{"http":{},"tcp":{},"udp":{}}`), emptyConf))
	require.NoError(t, store.Store(*emptyConf))
	require.Equal(t, []string{store.(*RedisStore).sk("kop_last_update")}, server.Keys())
}

func Test_redisStoreKeepConfAliveRebuildsEvictedOwnershipMember(t *testing.T) {
	server := miniredis.RunT(t)
	store := NewRedisStore("localhost", server.Addr(), 30, "", "", 0, nil, "", false)
	rs := store.(*RedisStore)

	conf := &dynamic.Configuration{}
	require.NoError(t, json.Unmarshal([]byte(NGINX_CONF_JSON), conf))
	require.NoError(t, store.Store(*conf))

	serviceKey := "traefik/http/services/nginx/loadBalancer/servers/0/url"
	server.Del(serviceKey)
	server.Del(rs.sk("http_services"))
	require.NoError(t, store.KeepConfAlive())

	value, err := store.Get(serviceKey)
	require.NoError(t, err)
	require.Equal(t, "http://172.20.0.2:80", value)
	members, err := server.SMembers(rs.sk("http_services"))
	require.NoError(t, err)
	require.Equal(t, []string{"nginx@docker"}, members)
}

func Test_redisStoreKeepConfAliveRenewsTransportKeys(t *testing.T) {
	server := miniredis.RunT(t)
	store := NewRedisStore("localhost", server.Addr(), 30, "", "", 0, nil, "", false)

	conf := &dynamic.Configuration{}
	require.NoError(t, json.Unmarshal([]byte(`{
		"http":{"serversTransports":{"custom":{"serverName":"http.example"}}},
		"tcp":{"serversTransports":{"custom":{"serverName":"tcp.example"}}},
		"udp":{}
	}`), conf))
	kv, err := ConfigToKV(*conf)
	require.NoError(t, err)
	require.NoError(t, store.Store(*conf))

	var transportKeys []string
	for key := range kv {
		if strings.Contains(key, "/serversTransports/") {
			transportKeys = append(transportKeys, key)
		}
	}
	require.NotEmpty(t, transportKeys)
	for _, key := range transportKeys {
		server.SetTTL(key, 30*time.Second)
	}
	server.FastForward(20 * time.Second)

	require.NoError(t, store.KeepConfAlive())
	for _, key := range transportKeys {
		require.Equal(t, 30*time.Second, server.TTL(key), key)
	}
}

func Test_redisStoreKeepConfAliveRenewsMismatchedKeyDuringRecovery(t *testing.T) {
	server := miniredis.RunT(t)
	store := NewRedisStore("localhost", server.Addr(), 30, "", "", 0, nil, "", false)
	rs := store.(*RedisStore)

	conf := &dynamic.Configuration{}
	require.NoError(t, json.Unmarshal([]byte(NGINX_CONF_JSON), conf))
	require.NoError(t, store.Store(*conf))

	ruleKey := "traefik/http/routers/nginx/rule"
	require.NoError(t, server.Set(ruleKey, "Host(`newer.local`)"))
	server.SetTTL(ruleKey, 5*time.Second)
	require.NoError(t, store.KeepConfAlive())
	value, err := store.Get(ruleKey)
	require.NoError(t, err)
	require.Equal(t, "Host(`newer.local`)", value)
	require.Equal(t, 30*time.Second, server.TTL(ruleKey))

	server.SetTTL(ruleKey, 5*time.Second)
	server.Del(rs.sk("kop_last_update"))

	require.NoError(t, store.KeepConfAlive())
	value, err = store.Get(ruleKey)
	require.NoError(t, err)
	require.Equal(t, "Host(`newer.local`)", value)
	require.Equal(t, 30*time.Second, server.TTL(ruleKey))
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

			server.FlushAll()
			rs.TTL = 0
			require.NoError(t, store.KeepConfAlive())
			require.True(t, server.Exists(localKey))
			require.False(t, server.Exists(peerKey))
		})
	}
}
