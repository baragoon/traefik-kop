func (s *DockerProxyServer) handleVersion(c fiber.Ctx) error {
	v, err := s.upstream.ServerVersion(context.Background())
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(v)
}

func (s *DockerProxyServer) handleContainersList(c fiber.Ctx) error {
	containers, err := s.upstream.ContainerList(context.Background(), container.ListOptions{})
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	// modify labels
	for _, container := range containers {
		container.Labels = s.filterLabels(container.Labels)
	}

	return c.JSON(containers)
}

func (s *DockerProxyServer) handleContainerInspect(c fiber.Ctx) error {
	container, err := s.upstream.ContainerInspect(context.Background(), c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	if container.Config != nil {
		container.Config.Labels = s.filterLabels(container.Config.Labels)
	}

	return c.JSON(container)
}

func (s *DockerProxyServer) handleEvents(c fiber.Ctx) error {
	var fa filters.Args
	f := c.Query("filters")
	if f != "" {
		var err error
		fa, err = filters.FromJSON(f)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
		}
	}

	eventsCh, errCh := s.upstream.Events(context.Background(), events.ListOptions{Filters: fa})

	c.Status(fiber.StatusOK).Context().SetBodyStreamWriter(fasthttp.StreamWriter(func(w *bufio.Writer) {
		encoder := json.NewEncoder(w)
		for {
			select {
			case event := <-eventsCh:
				if event.Type == "container" && event.Actor.Attributes != nil {
					event.Actor.Attributes = s.filterLabels(event.Actor.Attributes)
				}

				err := encoder.Encode(event)
				if err != nil {
					log.Error().Msgf("Error encoding event: %v", err)
				}
			case err := <-errCh:
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
	}))

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