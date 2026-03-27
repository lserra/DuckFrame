.PHONY: build test lint clean vet fmt tidy coverage

# Default target
all: fmt vet lint test build

# Build the project
build:
	CGO_ENABLED=1 go build ./...

# Run tests
test:
	CGO_ENABLED=1 go test ./... -v

# Run tests with coverage
coverage:
	CGO_ENABLED=1 go test ./... -coverprofile=coverage.out
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

# Format code
fmt:
	go fmt ./...

# Run go vet
vet:
	go vet ./...

# Run linter (requires golangci-lint)
lint:
	golangci-lint run ./...

# Tidy dependencies
tidy:
	go mod tidy

# Clean build artifacts
clean:
	go clean
	rm -f coverage.out coverage.html
