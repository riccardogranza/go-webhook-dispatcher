# syntax=docker/dockerfile:1

# ==============================================================================
# Stage 1: Static Binary Builder (Zero-CGO Pure Go + modernc.org/sqlite)
# ==============================================================================
FROM golang:1.24-alpine AS builder

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" -o /out/dispatcher ./cmd/dispatcher

# ==============================================================================
# Stage 2: Minimal Distroless Runtime (<15 MB Final Image)
# ==============================================================================
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /out/dispatcher /dispatcher

USER nonroot:nonroot
WORKDIR /home/nonroot

EXPOSE 8080

ENTRYPOINT ["/dispatcher"]
CMD ["-addr=:8080", "-db=/tmp/dispatcher.db", "-workers=64", "-queue-capacity=4096"]
