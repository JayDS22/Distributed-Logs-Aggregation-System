# syntax=docker/dockerfile:1.6
#
# Multi-stage build keeps the runtime image minimal (~25MB)

# --- build stage ---
FROM golang:1.21-alpine AS builder
WORKDIR /src
RUN apk add --no-cache git ca-certificates
COPY go.mod go.sum* ./
RUN go mod download || true
COPY . .
ARG TARGET=ingestor
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w -X main.version=$(git rev-parse --short HEAD 2>/dev/null || echo dev)" \
    -o /out/app ./cmd/${TARGET}

# --- runtime stage ---
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=builder /out/app /app/app
COPY --from=builder /src/configs /app/configs
COPY --from=builder /src/demo /app/demo
USER nonroot:nonroot
EXPOSE 8080 9090
ENTRYPOINT ["/app/app"]
CMD ["-config", "/app/configs/config.yaml"]
