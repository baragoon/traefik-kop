# Use a build arg to select which binary to copy into the image (set per-arch by the workflow)
ARG BIN_PATH
# Lightweight stage to provide CA certificates
FROM alpine:3.24 AS certs
RUN apk add --no-cache ca-certificates
# Final minimal runtime using distroless static
FROM gcr.io/distroless/static
# Copy CA bundle from the certs stage so Go's TLS verification has system roots
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
# Copy the binary specified by the build argument into the image
COPY ${BIN_PATH} /traefik-kop
ENTRYPOINT ["/traefik-kop"]