FROM golang:alpine@sha256:eb61c3873c97416fff0116553fff65d67a3549f72e11d9758849a689cdd8b523 AS builder

COPY * /go/
RUN go build -o helloworld ./

FROM alpine:latest@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf

COPY --from=builder /go/helloworld /helloworld

ENTRYPOINT ["/helloworld"]
