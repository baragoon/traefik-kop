FROM golang:alpine@sha256:fa3d596898d9f126af7c01d14ee7597bab3f39cccf392ef36b69fe77cd681f18 AS builder

COPY * /go/
RUN go build -o helloworld ./

FROM alpine:latest@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf

COPY --from=builder /go/helloworld /helloworld

ENTRYPOINT ["/helloworld"]
