FROM golang:alpine AS builder

WORKDIR /app

# Download dependencies first (caching)
COPY go.mod go.sum ./
RUN go mod download

# Copy source code and build
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o taskqueue cmd/main.go

# Minimal final image
FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/taskqueue .

# Expose port (default 8080)
EXPOSE 8080

CMD ["./taskqueue"]
