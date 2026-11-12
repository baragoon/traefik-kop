package traefikkop

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/traefik/traefik/v3/pkg/config/dynamic"
)

type TraefikStore interface {
	Store(conf dynamic.Configuration) error
	Get(key string) (string, error)
	Gets(key string) (map[string]string, error)
	Ping() error
	KeepConfAlive() error
}

func collectKeys(m interface{}) []string {
	mk := reflect.ValueOf(m).MapKeys()
	// set := mapset.NewSet()
	set := make([]string, len(mk))
	for i := 0; i < len(mk); i++ {
		// set.Add(mk[i].String())
		set[i] = mk[i].String()
	}
	return set
}

type RedisStore struct {
	Hostname string
	TTL      time.Duration // TTL in seconds, 0 means no TTL

	client     *redis.Client
	lastConfig *dynamic.Configuration
	localKV    map[string]interface{}
	// Ownership is captured before merging, keyed by the load balancer pointer.
	serverOwners     map[interface{}]map[string]struct{}
	lastServerOwners map[interface{}]map[string]struct{}
	mu               sync.RWMutex

	onStoreAttempt func()
}

func redisTLSConfigForAddr(addr string, tlsServerName ...string) *tls.Config {
	if addr == "" {
		return nil
	}

	host := addr
	if parsedHost, _, err := net.SplitHostPort(addr); err == nil && parsedHost != "" {
		host = parsedHost
	}
	if len(tlsServerName) > 0 {
		overrideName := strings.TrimSpace(tlsServerName[0])
		if overrideName != "" {
			host = overrideName
		}
	}

	return &tls.Config{
		ServerName: host,
		MinVersion: tls.VersionTLS12,
	}
}

func redisDialer(tlsServerName ...string) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		tlsConfig := redisTLSConfigForAddr(addr, tlsServerName...)
		if tlsConfig == nil {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		}
		return tls.DialWithDialer(&net.Dialer{}, network, addr, tlsConfig)
	}
}

func NewRedisStore(hostname string, addr string, ttl int, user string, pass string, db int, sentinelAddrs []string, sentinelMaster string, tlsEnabled bool, tlsServerName ...string) TraefikStore {
	var client *redis.Client

	directTLSConfig := (*tls.Config)(nil)
	if tlsEnabled {
		directTLSConfig = redisTLSConfigForAddr(addr, tlsServerName...)
	}

	if len(sentinelAddrs) > 0 && sentinelMaster != "" {
		log.Log().Msgf("creating new redis store via sentinel (master=%s, sentinels=%v) for hostname %s with %dsec TTL", sentinelMaster, sentinelAddrs, hostname, ttl)
		failoverOpts := &redis.FailoverOptions{
			MasterName:      sentinelMaster,
			SentinelAddrs:   sentinelAddrs,
			DisableIdentity: true,
			Username:        user,
			Password:        pass,
			DB:              db,
		}
		if tlsEnabled {
			failoverOpts.Dialer = redisDialer(tlsServerName...)
		}
		client = redis.NewFailoverClient(failoverOpts)
	} else {
		log.Log().Msgf("creating new redis store at %s for hostname %s with %dsec TTL", addr, hostname, ttl)
		client = redis.NewClient(&redis.Options{
			ClientName:      "",
			DisableIdentity: true,
			Addr:            addr,
			Username:        user,
			Password:        pass,
			DB:              db,
			TLSConfig:       directTLSConfig,
		})
	}

	store := &RedisStore{
		Hostname: hostname,
		TTL:      time.Duration(ttl) * time.Second,
		client:   client,
	}
	return store
}

func (s *RedisStore) Ping() error {
	return s.client.Ping(context.Background()).Err()
}

// sk returns the 'set key' for keeping track of our services/routers/middlewares
// e.g., traefik_http_routers@culture.local
func (s *RedisStore) sk(b string) string {
	return fmt.Sprintf("traefik_%s@%s", b, s.Hostname)
}

func (s *RedisStore) Get(key string) (string, error) {
	val, err := s.client.Get(context.Background(), key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", nil // key does not exist
		}
		return "", fmt.Errorf("failed to get key %s: %w", key, err)
	}
	return val, nil
}

func (s *RedisStore) Gets(key string) (map[string]string, error) {
	keys, err := s.client.Keys(context.Background(), key).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get keys matching %s: %w", key, err)
	}
	if len(keys) == 0 {
		return nil, nil // no keys found
	}
	vals := make(map[string]string)
	for _, k := range keys {
		val, err := s.Get(k)
		if err != nil {
			return nil, fmt.Errorf("failed to get value for key %s: %w", k, err)
		}
		vals[k] = val
	}
	return vals, nil
}

// traefik/http/services/gitea/loadBalancer/servers/0/url
// traefik/tcp/services/gitea-ssh/loadBalancer/servers/0/address
// traefik/udp/services/helloudp/loadBalancer/servers/0/address
var svcAddrRe = regexp.MustCompile(`^traefik/(tcp|udp|http)/services/([^/]+)/loadBalancer/servers/\d+/(address|url)$`)

func (s *RedisStore) recordServerOwners(lb interface{}, owners map[string]struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.serverOwners == nil {
		s.serverOwners = make(map[interface{}]map[string]struct{})
	}
	s.serverOwners[lb] = owners
}

// localConfigKV excludes every field of peer servers, preserving Redis indices.
// It must be called with mu held.
func (s *RedisStore) localConfigKV(conf dynamic.Configuration, kv map[string]interface{}) map[string]interface{} {
	local := make(map[string]interface{}, len(kv))
	currentOwners := make(map[interface{}]map[string]struct{})
	for key, value := range kv {
		local[key] = value
	}
	for proto, cfg := range map[string]interface{}{"http": conf.HTTP, "tcp": conf.TCP, "udp": conf.UDP} {
		v := reflect.ValueOf(cfg)
		if v.IsNil() {
			continue
		}
		services := v.Elem().FieldByName("Services")
		for _, name := range services.MapKeys() {
			svc := services.MapIndex(name)
			if svc.IsNil() {
				continue
			}
			lb := svc.Elem().FieldByName("LoadBalancer")
			if lb.IsNil() {
				continue
			}
			owners, tracked := s.serverOwners[lb.Interface()]
			if !tracked {
				owners, tracked = s.lastServerOwners[lb.Interface()]
			}
			if !tracked {
				continue
			}
			delete(s.serverOwners, lb.Interface())
			currentOwners[lb.Interface()] = owners
			servers := lb.Elem().FieldByName("Servers")
			for i := 0; i < servers.Len(); i++ {
				field := "Address"
				if proto == "http" {
					field = "URL"
				}
				if _, owned := owners[servers.Index(i).FieldByName(field).String()]; owned {
					continue
				}
				prefix := fmt.Sprintf("traefik/%s/services/%s/loadBalancer/servers/%d/", proto, stripDocker(name.String()), i)
				for key := range local {
					if strings.HasPrefix(key, prefix) {
						delete(local, key)
					}
				}
			}
		}
	}
	s.lastServerOwners = currentOwners
	return local
}

func (s *RedisStore) Store(conf dynamic.Configuration) error {
	if s.onStoreAttempt != nil {
		s.onStoreAttempt()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.storeLocked(conf)
}

// storeLocked must be called with mu held.
func (s *RedisStore) storeLocked(conf dynamic.Configuration) error {
	for _, entry := range []struct {
		values interface{}
		name   string
	}{
		{conf.HTTP.Middlewares, "http_middlewares"},
		{conf.HTTP.Routers, "http_routers"},
		{conf.HTTP.Services, "http_services"},
		{conf.TCP.Middlewares, "tcp_middlewares"},
		{conf.TCP.Routers, "tcp_routers"},
		{conf.TCP.Services, "tcp_services"},
		{conf.UDP.Routers, "udp_routers"},
		{conf.UDP.Services, "udp_services"},
	} {
		if err := s.removeOldKeys(entry.values, entry.name); err != nil {
			return fmt.Errorf("failed to update %s ownership: %w", entry.name, err)
		}
	}

	kv, err := ConfigToKV(conf)
	if err != nil {
		return err
	}

	sortedKeys := collectKeys(kv)
	sort.Strings(sortedKeys)
	for _, k := range sortedKeys {
		v := kv[k]
		log.Debug().Msgf("writing %s = %s", k, v)

		if matches := svcAddrRe.FindStringSubmatch(k); matches != nil {
			log.Info().Str("service", matches[2]).Str("service-type", matches[1]).Msgf("publishing %s", v)
		}

		if err := s.client.Set(context.Background(), k, v, s.TTL).Err(); err != nil {
			return fmt.Errorf("failed to set Redis key %s: %w", k, err)
		}
	}

	// Clean up stale loadBalancer/servers/N entries within services.
	// removeOldKeys() only diffs at the service name level, so when a service
	// goes from 2 servers to 1, the old servers/1/* keys are left behind.
	if err := s.removeStaleServerKeys(kv, conf.HTTP.Services, "http"); err != nil {
		return err
	}
	if err := s.removeStaleServerKeys(kv, conf.TCP.Services, "tcp"); err != nil {
		return err
	}
	if err := s.removeStaleServerKeys(kv, conf.UDP.Services, "udp"); err != nil {
		return err
	}

	for _, name := range []string{
		"http_middlewares", "http_routers", "http_services",
		"tcp_middlewares", "tcp_routers", "tcp_services",
		"udp_routers", "udp_services",
	} {
		if err := s.swapKeys(s.sk(name)); err != nil {
			return err
		}
	}

	// Update sentinel key with current timestamp
	if err := s.client.Set(context.Background(), s.sk("kop_last_update"), time.Now().Unix(), s.TTL).Err(); err != nil {
		return fmt.Errorf("failed to update Redis status key: %w", err)
	}

	s.localKV = s.localConfigKV(conf, kv)

	// Store a copy of the configuration in case redis restarts
	configCopy := *conf.DeepCopy()
	s.lastConfig = &configCopy

	return nil
}

// NeedsUpdate checks if Redis needs a full configuration refresh
// by checking for the sentinel key's existence
func (s *RedisStore) NeedsUpdate() bool {
	// Check if sentinel key exists
	exists, err := s.client.Exists(context.Background(), s.sk("kop_last_update")).Result()
	if err != nil {
		log.Warn().Msgf("Failed to check Redis status: %s", err)
	}
	return exists == 0
}

type ownershipSet struct {
	protocol string
	resource string
	members  []string
}

func (s *RedisStore) expectedOwnershipSets() map[string]ownershipSet {
	return map[string]ownershipSet{
		"http_middlewares": {"http", "middlewares", collectKeys(s.lastConfig.HTTP.Middlewares)},
		"http_routers":     {"http", "routers", collectKeys(s.lastConfig.HTTP.Routers)},
		"http_services":    {"http", "services", collectKeys(s.lastConfig.HTTP.Services)},
		"tcp_middlewares":  {"tcp", "middlewares", collectKeys(s.lastConfig.TCP.Middlewares)},
		"tcp_routers":      {"tcp", "routers", collectKeys(s.lastConfig.TCP.Routers)},
		"tcp_services":     {"tcp", "services", collectKeys(s.lastConfig.TCP.Services)},
		"udp_routers":      {"udp", "routers", collectKeys(s.lastConfig.UDP.Routers)},
		"udp_services":     {"udp", "services", collectKeys(s.lastConfig.UDP.Services)},
	}
}

func (s *RedisStore) ensureOwnershipSets(
	ctx context.Context,
	fullReset bool,
	presentKeys map[string]struct{},
) (map[string]map[string]struct{}, bool, error) {
	expectedSets := s.expectedOwnershipSets()
	currentSets := make(map[string]map[string]struct{}, len(expectedSets))
	rebuilt := false
	for name, expected := range expectedSets {
		members, err := s.client.SMembers(ctx, s.sk(name)).Result()
		if err != nil {
			return nil, false, fmt.Errorf("failed to read Redis ownership set %s: %w", name, err)
		}
		present := make(map[string]struct{}, len(members))
		for _, member := range members {
			present[member] = struct{}{}
		}
		currentSets[name] = present

		if len(expected.members) == 0 {
			continue
		}
		var missing []interface{}
		for _, member := range expected.members {
			if _, ok := present[member]; !ok {
				if fullReset {
					missing = append(missing, member)
					continue
				}
				prefix := fmt.Sprintf("traefik/%s/%s/%s/", expected.protocol, expected.resource, stripDocker(member))
				for key := range presentKeys {
					if strings.HasPrefix(key, prefix) {
						missing = append(missing, member)
						break
					}
				}
			}
		}
		if len(missing) > 0 {
			if err := s.client.SAdd(ctx, s.sk(name), missing...).Err(); err != nil {
				return nil, false, fmt.Errorf("failed to restore Redis ownership set %s: %w", name, err)
			}
			for _, member := range missing {
				memberName := member.(string)
				present[memberName] = struct{}{}
			}
			rebuilt = true
		}
	}

	return currentSets, rebuilt, nil
}

func redisValueString(value interface{}) string {
	switch value := value.(type) {
	case string:
		return value
	case []byte:
		return string(value)
	default:
		return fmt.Sprint(value)
	}
}

func isLocallyOwnedKey(key string, expectedSets map[string]ownershipSet, currentSets map[string]map[string]struct{}) bool {
	for name, expected := range expectedSets {
		for _, member := range expected.members {
			prefix := fmt.Sprintf("traefik/%s/%s/%s/", expected.protocol, expected.resource, stripDocker(member))
			if strings.HasPrefix(key, prefix) {
				_, ok := currentSets[name][member]
				return ok
			}
		}
	}
	return false
}

// Keep the cached configuration alive, restoring it if any key is missing.
func (s *RedisStore) KeepConfAlive() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lastConfig == nil {
		return nil // No config to push yet
	}

	ctx := context.Background()
	keys := collectKeys(s.localKV)
	sort.Strings(keys)
	readKeys := append(keys, s.sk("kop_last_update"))
	values, err := s.client.MGet(ctx, readKeys...).Result()
	if err != nil {
		return fmt.Errorf("failed to check Redis configuration: %w", err)
	}

	presentKeys := make(map[string]struct{}, len(keys))
	missing := make([]string, 0)
	mismatched := make(map[string]string)
	for i, key := range keys {
		value := values[i]
		if value == nil {
			missing = append(missing, key)
			continue
		}
		presentKeys[key] = struct{}{}
		actual := redisValueString(value)
		if actual != fmt.Sprint(s.localKV[key]) {
			mismatched[key] = actual
		}
	}
	markerMissing := values[len(keys)] == nil
	fullReset := markerMissing && len(presentKeys) == 0
	expectedSets := s.expectedOwnershipSets()
	currentSets, setsRebuilt, err := s.ensureOwnershipSets(ctx, fullReset, presentKeys)
	if err != nil {
		return err
	}
	recovering := len(missing) > 0 || markerMissing || setsRebuilt

	if recovering {
		log.Warn().Msg("Redis configuration is incomplete. Restoring missing local keys")
		for key, actual := range mismatched {
			s.localKV[key] = actual
		}
		for _, key := range missing {
			if !isLocallyOwnedKey(key, expectedSets, currentSets) {
				continue
			}
			created, err := s.client.SetNX(ctx, key, s.localKV[key], s.TTL).Result()
			if err != nil {
				return fmt.Errorf("failed to restore Redis key %s: %w", key, err)
			}
			if !created {
				value, err := s.client.Get(ctx, key).Result()
				if err != nil {
					return fmt.Errorf("failed to read Redis key %s after concurrent restore: %w", key, err)
				}
				s.localKV[key] = value
			}
		}
	} else {
		log.Debug().Msg("Redis configuration keys match cached configuration")
	}

	pipe := s.client.Pipeline()
	expirations := make(map[string]*redis.BoolCmd)
	for _, key := range keys {
		if !isLocallyOwnedKey(key, expectedSets, currentSets) {
			continue
		}
		if _, mismatch := mismatched[key]; mismatch {
			if recovering {
				continue
			}
			pipe.Set(ctx, key, s.localKV[key], s.TTL)
			continue
		}
		if s.TTL > 0 {
			expirations[key] = pipe.Expire(ctx, key, s.TTL)
		}
	}

	markerKey := s.sk("kop_last_update")
	var markerExpiration *redis.BoolCmd
	if markerMissing {
		if _, err := s.client.SetNX(ctx, markerKey, time.Now().Unix(), s.TTL).Result(); err != nil {
			return fmt.Errorf("failed to restore Redis status key: %w", err)
		}
	} else if s.TTL > 0 {
		markerExpiration = pipe.Expire(ctx, markerKey, s.TTL)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to refresh Redis configuration: %w", err)
	}

	if s.TTL > 0 {
		for key, expiration := range expirations {
			if expiration.Val() {
				continue
			}
			created, err := s.client.SetNX(ctx, key, s.localKV[key], s.TTL).Result()
			if err != nil {
				return fmt.Errorf("failed to restore Redis key %s: %w", key, err)
			}
			if !created {
				value, err := s.client.Get(ctx, key).Result()
				if err != nil {
					return fmt.Errorf("failed to read Redis key %s after concurrent restore: %w", key, err)
				}
				s.localKV[key] = value
			}
		}
		if markerExpiration != nil && !markerExpiration.Val() {
			if _, err := s.client.SetNX(ctx, markerKey, time.Now().Unix(), s.TTL).Result(); err != nil {
				return fmt.Errorf("failed to restore Redis status key: %w", err)
			}
		}
	}

	return nil
}

func (s *RedisStore) swapKeys(setkey string) error {
	// store router name list by renaming
	err := s.client.Rename(context.Background(), setkey+"_new", setkey).Err()
	if err != nil {
		if strings.Contains(err.Error(), "no such key") {
			if err := s.client.Unlink(context.Background(), setkey).Err(); err != nil {
				return fmt.Errorf("unlink failed: %w", err)
			}
			return nil
		}
		return fmt.Errorf("rename failed: %w", err)
	}
	return nil
}

// k returns the actual config key path
// e.g., traefik/http/routers/nginx@docker
func (s *RedisStore) k(sk, b string) string {
	k := strings.ReplaceAll(fmt.Sprintf("traefik_%s", sk), "_", "/")
	b = stripDocker(b)
	return fmt.Sprintf("%s/%s", k, b)
}

func (s *RedisStore) removeKeys(setkey string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	log.Debug().Func(func(e *zerolog.Event) {
		e.Msgf("removing keys from %s: %s", setkey, strings.Join(keys, ","))
	})
	for _, removeKey := range keys {
		keyPath := s.k(setkey, removeKey) + "/*"
		log.Debug().Msgf("removing keys matching %s", keyPath)
		res, err := s.client.Keys(context.Background(), keyPath).Result()
		if err != nil {
			return fmt.Errorf("fetch failed: %w", err)
		}
		if err := s.client.Unlink(context.Background(), res...).Err(); err != nil {
			return fmt.Errorf("unlink failed: %w", err)
		}
	}
	return nil
}

func (s *RedisStore) removeOldKeys(m interface{}, setname string) error {
	setkey := s.sk(setname)
	if err := s.client.Del(context.Background(), setkey+"_new").Err(); err != nil {
		return fmt.Errorf("clear temporary ownership set: %w", err)
	}

	// store new keys in temp set
	newkeys := collectKeys(m)
	if len(newkeys) == 0 {
		res, err := s.client.SMembers(context.Background(), setkey).Result()
		if err != nil {
			return fmt.Errorf("fetch failed: %w", err)
		}
		return s.removeKeys(setname, res)

	} else {
		// make a diff and remove
		err := s.client.SAdd(context.Background(), setkey+"_new", mkslice(newkeys)...).Err()
		if err != nil {
			return fmt.Errorf("add failed: %w", err)
		}

		// diff the existing keys with the new ones
		res, err := s.client.SDiff(context.Background(), setkey, setkey+"_new").Result()
		if err != nil {
			return fmt.Errorf("diff failed: %w", err)
		}
		return s.removeKeys(setname, res)
	}
}

func (s *RedisStore) removeStaleServerKeys(kv map[string]interface{}, services interface{}, proto string) error {
	ctx := context.Background()
	svcMap := reflect.ValueOf(services)
	if svcMap.Kind() != reflect.Map {
		return nil
	}

	for _, nameVal := range svcMap.MapKeys() {
		name := stripDocker(nameVal.String())
		prefix := fmt.Sprintf("traefik/%s/services/%s/loadBalancer/servers/", proto, name)

		existingKeys, err := s.client.Keys(ctx, prefix+"*").Result()
		if err != nil {
			return fmt.Errorf("failed to find server keys for service %s: %w", name, err)
		}
		for _, key := range existingKeys {
			if _, exists := kv[key]; !exists {
				log.Debug().Msgf("removing stale server key: %s", key)
				if err := s.client.Unlink(ctx, key).Err(); err != nil {
					return fmt.Errorf("failed to remove stale server key %s: %w", key, err)
				}
			}
		}
	}
	return nil
}

// mkslice converts a string slice to an interface slice
func mkslice(old []string) []interface{} {
	new := make([]interface{}, len(old))
	for i, v := range old {
		new[i] = v
	}
	return new
}
