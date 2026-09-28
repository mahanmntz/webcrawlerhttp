# Build Stage
FROM golang:1.23-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o crawler-engine .

# Final Minimal Distroless / Alpine Image
FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app
COPY --from=builder /app/crawler-engine .

ENV REDIS_ADDR=redis:6379
ENV WORKER_COUNT=5
ENV FETCH_TIMEOUT_SEC=10
ENV POLITENESS_DELAY_MS=1000

CMD ["./crawler-engine"]
