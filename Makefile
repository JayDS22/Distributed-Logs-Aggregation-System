# LogStream Makefile
# Common developer commands. Run `make help` for the full list.

GO              ?= go
BIN_DIR         := bin
INGESTOR_BIN    := $(BIN_DIR)/ingestor
ALERTER_BIN     := $(BIN_DIR)/alerter
LOADGEN_BIN     := $(BIN_DIR)/loadgen
DOCKER_REPO     ?= ghcr.io/jayds22
VERSION         ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)

.PHONY: help
help: ## list commands
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: tidy
tidy: ## go mod tidy
	$(GO) mod tidy

.PHONY: build
build: ## build all binaries
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 $(GO) build -ldflags="-s -w" -o $(INGESTOR_BIN) ./cmd/ingestor
	CGO_ENABLED=0 $(GO) build -ldflags="-s -w" -o $(ALERTER_BIN)  ./cmd/alerter
	CGO_ENABLED=0 $(GO) build -ldflags="-s -w" -o $(LOADGEN_BIN)  ./cmd/loadgen

.PHONY: test
test: ## run unit tests
	$(GO) test -race -count=1 ./internal/... ./pkg/... ./tests/unit/...

.PHONY: test-integration
test-integration: ## run integration tests (requires MONGODB_TEST_URI)
	$(GO) test -count=1 ./tests/integration/...

.PHONY: cover
cover: ## generate coverage report
	$(GO) test -coverprofile=coverage.out ./internal/... ./pkg/... ./tests/unit/...
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "Open coverage.html"

.PHONY: vet
vet: ## go vet
	$(GO) vet ./...

.PHONY: lint
lint: ## run staticcheck if available
	@command -v staticcheck >/dev/null && staticcheck ./... || echo "(staticcheck not installed; skipping)"

.PHONY: run
run: build ## run ingestor locally
	$(INGESTOR_BIN) -config configs/config.yaml

.PHONY: docker-build
docker-build: ## build docker images
	docker build -f deployments/docker/Dockerfile --build-arg TARGET=ingestor \
	  -t $(DOCKER_REPO)/logstream-ingestor:$(VERSION) .
	docker build -f deployments/docker/Dockerfile --build-arg TARGET=alerter  \
	  -t $(DOCKER_REPO)/logstream-alerter:$(VERSION) .

.PHONY: compose-up
compose-up: ## bring up docker-compose stack
	docker compose -f deployments/docker/docker-compose.yml up --build -d
	@echo ""
	@echo "Stack is up. URLs:"
	@echo "  API:        http://localhost:8080/api/v1/logs"
	@echo "  Demo:       http://localhost:8080/demo/"
	@echo "  Metrics:    http://localhost:9090/metrics"
	@echo "  Prometheus: http://localhost:9095"
	@echo "  Grafana:    http://localhost:3000 (admin/admin)"

.PHONY: compose-down
compose-down: ## tear down docker-compose stack
	docker compose -f deployments/docker/docker-compose.yml down -v

.PHONY: load
load: build ## run quick load test (10s @ 5k rps)
	$(LOADGEN_BIN) -rps 5000 -duration 10s

.PHONY: clean
clean: ## clean build artifacts
	rm -rf $(BIN_DIR) coverage.out coverage.html
