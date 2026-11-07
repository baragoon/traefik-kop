FROM alpine:3.24@sha256:3f4d6b32e26703c6082839234f43769d5046b3ac7c910033911d1e71d8cd630a AS certs
RUN apk add --no-cache ca-certificates

FROM scratch
ENTRYPOINT ["/traefik-kop"]
ARG TARGETPLATFORM
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY ${TARGETPLATFORM}/traefik-kop /traefik-kop