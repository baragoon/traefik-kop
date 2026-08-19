# Lightweight stage to provide CA certificates
FROM alpine:3.24 AS certs
RUN apk add --no-cache ca-certificates

# Final runtime: use a small shell-equipped base so we can choose the correct
# architecture binary at container startup. This avoids relying on build-time
# build args from the release pipeline which can vary between environments.
FROM alpine:3.24
RUN apk add --no-cache ca-certificates
# Copy CA bundle from the certs stage so Go's TLS verification has system roots
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

# Copy all known architecture binaries into distinct paths. Use explicit paths
# so the build won't accidentally overwrite files when multiple archs are present
# in the build context.
COPY linux/amd64/traefik-kop /opt/traefik-kop/amd64/traefik-kop
COPY linux/arm64/traefik-kop /opt/traefik-kop/arm64/traefik-kop

# Add a small entrypoint that chooses the correct binary at runtime based on
# uname -m. This keeps the image runnable even if the build context contained
# multiple architectures.
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["-V"]