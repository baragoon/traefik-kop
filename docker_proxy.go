package traefikkop

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/logger"
	"github.com/moby/moby/client"
	"github.com/rs/zerolog/log"
)

func getAvailablePort() (net.Listener, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return nil, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, err
	}
	return l, nil
}

// DockerProxyServer is a proxy server that filters docker labels based on a prefix
type DockerProxyServer struct {
	upstream    client.APIClient
	labelPrefix string
}

func createProxy(upstream client.APIClient, labelPrefix string) *DockerProxyServer {
	if labelPrefix != "" && !strings.HasSuffix(labelPrefix, ".") {
		labelPrefix += "."
	}
	return &DockerProxyServer{
		upstream:    upstream,
		labelPrefix: labelPrefix,
	}
}

func (s *DockerProxyServer) filterLabels(labels map[string]string) map[string]string {
	if s.labelPrefix == "" || labels == nil {
		return labels
	}

	newLabels := make(map[string]string)

	for k, v := range labels {
		if strings.HasPrefix(k, s.labelPrefix) {
			// strip prefix from our labels
			newKey := strings.TrimPrefix(k, s.labelPrefix)
			newLabels[newKey] = v
		} else if !strings.HasPrefix(k, "traefik.") {
			// keep every other than traefik.* labels as is
			newLabels[k] = v
		}
	}

	return newLabels
}

func (s *DockerProxyServer) handleVersion(c fiber.Ctx) error {
	v, err := s.upstream.ServerVersion(context.Background(), client.ServerVersionOptions{})
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(v)
}

func (s *DockerProxyServer) handleContainersList(c fiber.Ctx) error {
	result, err := s.upstream.ContainerList(context.Background(), client.ContainerListOptions{})
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	// modify labels
	for _, container := range result.Items {
		container.Labels = s.filterLabels(container.Labels)
	}

	return c.JSON(result.Items)
}

func (s *DockerProxyServer) handleContainerInspect(c fiber.Ctx) error {
	result, err := s.upstream.ContainerInspect(context.Background(), c.Params("id"), client.ContainerInspectOptions{})
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	container := result.Container
	if container.Config != nil {
		container.Config.Labels = s.filterLabels(container.Config.Labels)
	}

	return c.JSON(container)
}

func (s *DockerProxyServer) handleEvents(c fiber.Ctx) error {
	var filters client.Filters
	f := c.Query("filters")
	if f != "" {
		if err := json.Unmarshal([]byte(f), &filters); err != nil {
			var legacyFilters map[string][]string
			if legacyErr := json.Unmarshal([]byte(f), &legacyFilters); legacyErr != nil {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
			}
			filters = make(client.Filters, len(legacyFilters))
			for key, values := range legacyFilters {
				filters.Add(key, values...)
			}
		}
	}

	result := s.upstream.Events(context.Background(), client.EventsListOptions{Filters: filters})

	c.Status(fiber.StatusOK)
	c.SendStreamWriter(func(w *bufio.Writer) {
		encoder := json.NewEncoder(w)
		for {
			select {
			case event := <-result.Messages:
				if event.Type == "container" && event.Actor.Attributes != nil {
					event.Actor.Attributes = s.filterLabels(event.Actor.Attributes)
				}

				err := encoder.Encode(event)
				if err != nil {
					log.Error().Msgf("Error encoding event: %v", err)
				}
			case err := <-result.Err:
				if err != nil {
					e := encoder.Encode(err)
					if e != nil {
						log.Error().Msgf("Error encoding error: %v", e)
					}
					return
				}
			}
			err := w.Flush()
			if err != nil {
				break
			}
		}
	})

	return nil
}

func (s *DockerProxyServer) handleNotFound(c fiber.Ctx) error {
	log.Warn().Msgf("Unhandled request: %s %s", c.Method(), c.OriginalURL())
	return c.Status(fiber.StatusNotFound).SendString("Not Found")
}

func (s *DockerProxyServer) start() (*fiber.App, string) {
	app := fiber.New()
	if os.Getenv("DEBUG") != "" {
		app.Use(logger.New())
	}

	app.All("/_ping", func(ctx fiber.Ctx) error { return ctx.SendStatus(fiber.StatusNoContent) })
	app.Get("/v*/version", s.handleVersion)
	app.Get("/v*/containers/json", s.handleContainersList)
	app.Get("/v*/containers/:id/json", s.handleContainerInspect)
	app.Get("/v*/events", s.handleEvents)
	app.All("/*", s.handleNotFound)

	listener, err := getAvailablePort()
	if err != nil {
		log.Fatal().Err(err)
	}

	go app.Listener(listener, fiber.ListenConfig{DisableStartupMessage: true})

	dockerEndpoint := fmt.Sprintf("http://localhost:%d", listener.Addr().(*net.TCPAddr).Port)
	log.Debug().Msgf("Started docker proxy at %s with label prefix '%s'", dockerEndpoint, s.labelPrefix)

	return app, dockerEndpoint
}
