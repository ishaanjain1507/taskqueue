FROM golang:alpine AS builder

WORKDIR /app

# Some Alpine-based Go tags ship without a populated trust store. Bootstrap
# the signed CA package over HTTP, then immediately restore HTTPS repositories.
RUN sed -i 's|https://|http://|g' /etc/apk/repositories \
    && rm -f /etc/ca-certificates.conf \
        /etc/ssl/certs/ca-certificates.crt \
        /etc/apk/protected_paths.d/ca-certificates.list \
        /etc/ca-certificates/update.d/certhash \
    && apk add --no-cache ca-certificates \
    && sed -i 's|http://|https://|g' /etc/apk/repositories

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
COPY --from=builder /app/web ./web

# Expose port (default 8080)
EXPOSE 8080

CMD ["./taskqueue"]
