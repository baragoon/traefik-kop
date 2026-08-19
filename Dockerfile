ARG TARGETPLATFORM
# Lightweight stage to provide CA certificates
FROM alpine:3.24 AS certs
RUN apk add --no-cache ca-certificates
# Final minimal runtime using distroless static
FROM gcr.io/distroless/static
# Copy CA bundle from the certs stage so Go's TLS verification has system roots
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
# Copy platform-specific binary produced by the build pipeline
# Copy the platform-specific binary produced by the build pipeline.
# Use a wildcard so CI contexts that include a single platform folder (e.g. linux/arm64)
# are handled even when TARGETPLATFORM isn't defined by the builder.
COPY linux/*/traefik-kop /traefik-kop
ENTRYPOINT ["/traefik-kop"]