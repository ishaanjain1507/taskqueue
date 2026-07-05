.PHONY: up down build test lint loadtest clean

# Build the main API server
build:
	go build -o bin/api cmd/main.go

# Run the docker-compose setup in detached mode
up:
	docker compose up -d --build

# Stop the docker-compose setup
down:
	docker compose down

# Run all tests
test:
	go test -v ./...

# Run the linter
lint:
	golangci-lint run ./...

# Run the native Go load test
loadtest:
	go run cmd/loadtest/main.go

# Clean build artifacts
clean:
	rm -rf bin/
