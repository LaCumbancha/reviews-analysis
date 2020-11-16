FROM golang:1.14

RUN mkdir -p /build
WORKDIR /build/
COPY . .

# CGO_ENABLED must be disabled to run Go binary in Alpine
RUN CGO_ENABLED=0 GOOS=linux go build -mod vendor -o bin/manager github.com/LaCumbancha/backup-server/backup-manager

FROM busybox:latest
COPY --from=0 /build/bin/manager /manager
ENTRYPOINT ["/bin/sh"]
