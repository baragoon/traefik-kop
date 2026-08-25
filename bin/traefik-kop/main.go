package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"

	traefikkop "github.com/baragoon/traefik-kop"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"
)

const defaultDockerHost = "unix:///var/run/docker.sock"

var (
	version string
	commit  string
	date    string
)

func printVersion(cmd *cli.Command) {
	fmt.Printf("%s version %s (commit: %s, built %s)\n", cmd.Name, cmd.Version, commit, date)
}

func flags() {
	if version == "" {
		version = "n/a"
	}
	if commit == "" {
		commit = "head"
	}
	if date == "" {
		date = "n/a"
	}

	cli.VersionFlag = &cli.BoolFlag{
		Name:    "version",
		Aliases: []string{"V"},
		Usage:   "Print the version",
	}
	cli.VersionPrinter = printVersion

	app := &cli.Command{
		Name:    "traefik-kop",
		Usage:   "A dynamic docker->redis->traefik discovery agent",
		Version: version,

		Action: doStart,

		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "hostname",
				Usage:   "Hostname to identify this node in redis",
				Value:   getHostname(),
				Sources: cli.EnvVars("KOP_HOSTNAME"),
			},
			&cli.StringFlag{
				Name:    "bind-ip",
				Usage:   "IP address to bind services to",
				Sources: cli.EnvVars("BIND_IP"),
			},
			&cli.StringFlag{
				Name:    "bind-interface",
				Usage:   "Network interface to derive bind IP (overrides auto-detect)",
				Sources: cli.EnvVars("BIND_INTERFACE"),
			},
			&cli.BoolFlag{
				Name:    "skip-replace",
				Usage:   "Disable custom IP replacement",
				Sources: cli.EnvVars("SKIP_REPLACE"),
			},
			&cli.StringFlag{
				Name:    "redis-addr",
				Usage:   "Redis address",
				Value:   "127.0.0.1:6379",
				Sources: cli.EnvVars("REDIS_ADDR"),
			},
			&cli.StringFlag{
				Name:    "redis-user",
				Usage:   "Redis username",
				Value:   "default",
				Sources: cli.EnvVars("REDIS_USER"),
			},
			&cli.StringFlag{
				Name:    "redis-pass",
				Usage:   "Redis password (if needed)",
				Sources: cli.EnvVars("REDIS_PASS"),
			},
			&cli.IntFlag{
				Name:    "redis-db",
				Usage:   "Redis DB number",
				Value:   0,
				Sources: cli.EnvVars("REDIS_DB"),
			},
			&cli.BoolFlag{
				Name:    "redis-tls",
				Usage:   "Connect to Redis over TLS",
				Value:   false,
				Sources: cli.EnvVars("REDIS_TLS"),
			},
			&cli.StringFlag{
				Name:    "redis-tls-server-name",
				Usage:   "Override the TLS ServerName used when connecting to Redis over TLS; defaults to the Redis address host",
				Sources: cli.EnvVars("REDIS_TLS_SERVER_NAME"),
			},
			&cli.IntFlag{
				Name:    "redis-ttl",
				Usage:   "Redis TTL (in seconds)",
				Value:   0,
				Sources: cli.EnvVars("REDIS_TTL"),
			},
			&cli.StringFlag{
				Name:    "redis-sentinel-addrs",
				Usage:   "Comma-separated list of Redis Sentinel addresses (e.g., host1:26379,host2:26379)",
				Sources: cli.EnvVars("REDIS_SENTINEL_ADDRS"),
			},
			&cli.StringFlag{
				Name:    "redis-sentinel-master",
				Usage:   "Redis Sentinel master name",
				Sources: cli.EnvVars("REDIS_SENTINEL_MASTER"),
			},
			&cli.StringFlag{
				Name:    "docker-host",
				Usage:   "Docker endpoint",
				Value:   defaultDockerHost,
				Sources: cli.EnvVars("DOCKER_ADDR", "DOCKER_HOST"),
			},
			&cli.StringFlag{
				Name:    "docker-config",
				Usage:   "Docker provider config (file must end in .yaml)",
				Sources: cli.EnvVars("DOCKER_CONFIG"),
			},
			&cli.StringFlag{
				Name:    "docker-prefix",
				Usage:   "Docker label prefix",
				Sources: cli.EnvVars("DOCKER_PREFIX"),
			},
			&cli.Int64Flag{
				Name:    "poll-interval",
				Usage:   "Poll interval for refreshing container list",
				Value:   60,
				Sources: cli.EnvVars("KOP_POLL_INTERVAL"),
			},
			&cli.StringFlag{
				Name:    "namespace",
				Usage:   "Namespace to process containers for",
				Sources: cli.EnvVars("NAMESPACE"),
			},
			&cli.BoolFlag{
				Name:    "verbose",
				Usage:   "Enable debug logging",
				Value:   false,
				Sources: cli.EnvVars("VERBOSE", "DEBUG"),
			},
		},
	}

	err := app.Run(context.Background(), os.Args)
	if err != nil {
		log.Fatal().Err(err).Msg("Application error")
	}
}

func setupLogging(debug bool) {
	if debug {
		logrus.SetLevel(logrus.DebugLevel)
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
		log.Logger = log.Logger.Level(zerolog.DebugLevel)
	} else {
		logrus.SetLevel(logrus.InfoLevel)
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
		log.Logger = log.Logger.Level(zerolog.InfoLevel)
	}
	zerolog.DefaultContextLogger = &log.Logger

	formatter := &logrus.TextFormatter{DisableColors: true, FullTimestamp: true, DisableSorting: true}
	logrus.SetFormatter(formatter)
}

func main() {
	flags()
}

func splitStringArr(str string) []string {
	trimmed := strings.TrimSpace(str)
	if trimmed != "" {
		trimmedVals := strings.Split(trimmed, ",")
		splitArr := make([]string, len(trimmedVals))
		for i, v := range trimmedVals {
			splitArr[i] = strings.TrimSpace(v)
		}
		return splitArr
	}
	return []string{}
}

func doStart(ctx context.Context, cmd *cli.Command) error {
	traefikkop.Version = version

	namespaces := splitStringArr(cmd.String("namespace"))

	// Determine bind IP: precedence -> explicit --bind-ip -> --bind-interface -> auto-detect
	bindIP := strings.TrimSpace(cmd.String("bind-ip"))
	if bindIP == "" {
		iface := strings.TrimSpace(cmd.String("bind-interface"))
		bindIP = getDefaultIP(iface)
	}

	sentinelAddrs := splitStringArr(cmd.String("redis-sentinel-addrs"))

	config := traefikkop.Config{
		Hostname:            cmd.String("hostname"),
		BindIP:              bindIP,
		SkipReplace:         cmd.Bool("skip-replace"),
		RedisAddr:           cmd.String("redis-addr"),
		RedisUser:           cmd.String("redis-user"),
		RedisPass:           cmd.String("redis-pass"),
		RedisDB:             int(cmd.Int("redis-db")),
		RedisTLS:            cmd.Bool("redis-tls"),
		RedisTLSServerName:  strings.TrimSpace(cmd.String("redis-tls-server-name")),
		RedisTTL:            int(cmd.Int("redis-ttl")),
		RedisSentinelAddrs:  sentinelAddrs,
		RedisSentinelMaster: cmd.String("redis-sentinel-master"),
		DockerHost:          cmd.String("docker-host"),
		DockerConfig:        cmd.String("docker-config"),
		DockerPrefix:        cmd.String("docker-prefix"),
		PollInterval:        cmd.Int64("poll-interval"),
		Namespace:           namespaces,
	}

	if config.BindIP == "" {
		log.Fatal().Msg("Bind IP cannot be empty")
	}

	if len(config.RedisSentinelAddrs) > 0 && config.RedisSentinelMaster == "" {
		log.Fatal().Msg("--redis-sentinel-master is required when using --redis-sentinel-addrs")
	}
	if config.RedisSentinelMaster != "" && len(config.RedisSentinelAddrs) == 0 {
		log.Fatal().Msg("--redis-sentinel-addrs is required when using --redis-sentinel-master")
	}

	if os.Getenv("DOCKER_HOST") != "" {
		if config.DockerPrefix != "" {
			log.Fatal().Msgf("Using the DOCKER_HOST env var with the DOCKER PREFIX feature is not allowed; use DOCKER_ADDR to set the Docker endpoint instead")
		} else if config.DockerHost != os.Getenv("DOCKER_HOST") {
			log.Warn().Msgf("Setting the DOCKER_HOST env var in addition to the --docker-host flag or DOCKER_ADDR env var is not recommended and may cause unexpected behavior.")
		}
	}

	setupLogging(cmd.Bool("verbose"))
	log.Debug().Msgf("using traefik-kop config: %s", fmt.Sprintf("%+v", config))

	traefikkop.Start(config)
	return nil
}

func getHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "traefik-kop"
	}
	return hostname
}

func getDefaultIP(iface string) string {
	// If a network interface is specified, attempt to get its primary IPv4 address
	if strings.TrimSpace(iface) != "" {
		if ip := GetInterfaceIP(iface); ip != nil {
			return ip.String()
		}
		log.Warn().Msgf("failed to get IP for interface '%s'; falling back to auto-detect", iface)
	}
	ip := GetOutboundIP()
	if ip == nil {
		return ""
	}
	return ip.String()
}

// Get preferred outbound ip of this machine
// via https://stackoverflow.com/a/37382208/102920
func GetOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Warn().Msgf("failed to detect outbound IP: %s", err)
		return nil
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}

// GetInterfaceIP returns the first non-loopback IPv4 address for the named interface
func GetInterfaceIP(name string) net.IP {
	iface, err := net.InterfaceByName(name)
	if err != nil {
		log.Warn().Msgf("unable to find interface '%s': %v", name, err)
		return nil
	}
	addrs, err := iface.Addrs()
	if err != nil {
		log.Warn().Msgf("unable to list addresses for interface '%s': %v", name, err)
		return nil
	}
	for _, a := range addrs {
		var ip net.IP
		switch v := a.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}
		if ip == nil || ip.IsLoopback() {
			continue
		}
		ip = ip.To4()
		if ip == nil {
			continue // skip IPv6 for bind default
		}
		return ip
	}
	return nil
}
