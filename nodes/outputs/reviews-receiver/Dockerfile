FROM golang:1.14

RUN mkdir -p /build
WORKDIR /build/
COPY . .

# CGO_ENABLED must be disabled to run Go binary in Alpine
RUN CGO_ENABLED=0 GOOS=linux go build -mod vendor -o bin/receiver github.com/LaCumbancha/reviews-analysis/nodes/outputs/reviews-receiver

FROM busybox:latest
COPY --from=0 /build/bin/receiver /receiver
ENTRYPOINT ["/bin/sh"]
