package traefikkop

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/traefik/traefik/v3/pkg/provider/docker"
	"gopkg.in/yaml.v3"
)

type Config struct {
	DockerConfig string
	DockerHost   string

	// prefix for traefik labels to accept
	DockerPrefix       string
	Hostname           string
	BindIP             string
	SkipReplace        bool
	RedisAddr          string
	RedisTTL           int
	RedisUser          string
	RedisPass          string
	RedisDB            int
	RedisTLS           bool
	RedisTLSServerName string

	// Redis Sentinel
	RedisSentinelAddrs  []string
	RedisSentinelMaster string

	PollInterval int64
	Namespace    []string
}

type ConfigFile struct {
	Docker docker.Provider `yaml:"docker"`
}

func loadDockerConfig(input string) (*docker.Provider, error) {
	if input == "" {
		return nil, nil
	}

	var r io.Reader

	if looksLikeFile(input) {
		// see if given filename
		_, err := os.Stat(input)
		if err == nil {
			log.Debug().Msgf("loading docker config from file %s", input)
			r, err = os.Open(input)
			if err != nil {
				return nil, fmt.Errorf("failed to open docker config %s: %w", input, err)
			}
			defer r.(io.Closer).Close()
		} else {
			return nil, fmt.Errorf("failed to stat docker config %s: %w", input, err)
		}
	} else {
		log.Debug().Msgf("loading docker config from yaml input")
		r = strings.NewReader(input) // treat as direct yaml input
	}

	// parse
	conf := ConfigFile{Docker: docker.Provider{}}
	err := yaml.NewDecoder(r).Decode(&conf)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	conf.Docker.Network = strings.TrimSpace(conf.Docker.Network)

	return &conf.Docker, nil
}

func looksLikeFile(input string) bool {
	if strings.Contains(input, "\n") {
		return false
	}
	ok, _ := regexp.MatchString(`\.ya?ml`, input)
	return ok
}
