FROM alpine:3.24 AS certs
RUN apk add --no-cache ca-certificates

FROM scratch
ENTRYPOINT ["/traefik-kop"]
ARG TARGETPLATFORM
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY ${TARGETPLATFORM}/traefik-kop /traefik-kop