.PHONY: dev dev-build dev-down dev-shell build build-local build-linux-amd64 package-deb test test-unit test-integration coverage fmt fmt-staged lint lint-staged hooks clean

# Version info from git
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
ARCH    ?= amd64

LDFLAGS := -X github.com/hieutdo/policyfs/internal/cli.Version=$(VERSION) \
           -X github.com/hieutdo/policyfs/internal/cli.Commit=$(COMMIT) \
           -X github.com/hieutdo/policyfs/internal/cli.BuildDate=$(DATE)

DCD ?= docker compose -f docker-compose.dev.yaml
DCD_EXEC ?= $(DCD) exec dev
DCD_EXEC_T ?= $(DCD) exec -T dev

dev: dev-build dev-up

dev-build:
	$(DCD) build

dev-up:
	$(DCD) up -d

dev-down:
	$(DCD) down

dev-fresh:
	$(DCD) down -v
	$(MAKE) dev

dev-logs:
	$(DCD) logs

dev-shell:
	$(DCD_EXEC) bash

dev-watch:
	$(DCD_EXEC) /go/bin/air -c /workspace/.air.toml

dev-dlv:
	$(DCD_EXEC) bash /workspace/scripts/dlv.sh code

dev-dlv-test:
	$(DCD_EXEC) bash /workspace/scripts/dlv.sh unit

dev-dlv-test-integration:
	$(DCD_EXEC) bash /workspace/scripts/dlv.sh integration

dev-dlv-stop:
	$(DCD_EXEC) pkill -f '/go/bin/dlv' || true

test-unit:
	$(DCD_EXEC) go test -v ./...

test-integration:
	$(DCD_EXEC) go test -v -tags=integration ./tests/integration/...

coverage:
	$(DCD_EXEC_T) bash /workspace/scripts/coverage.sh

build:
	$(DCD_EXEC) go build -ldflags "$(LDFLAGS)" -o bin/pfs ./cmd/pfs

build-local:
	go build -ldflags "$(LDFLAGS)" -o bin/pfs ./cmd/pfs

build-linux-amd64:
	LDFLAGS="$(LDFLAGS)" \
	DOCKER_DEFAULT_PLATFORM=linux/amd64 \
	$(DCD) run --rm --no-deps -e LDFLAGS \
		dev bash -lc 'GOOS=linux GOARCH=amd64 CGO_ENABLED=1 /usr/local/go/bin/go build -ldflags "$${LDFLAGS}" -o bin/pfs ./cmd/pfs'

package-deb: build-linux-amd64
	VERSION="$(VERSION)" ARCH="$(ARCH)" \
	DOCKER_DEFAULT_PLATFORM=linux/amd64 \
	$(DCD) run --rm --no-deps -e VERSION -e ARCH \
		dev bash /workspace/scripts/package_deb.sh

fmt:
	$(DCD_EXEC_T) bash /workspace/scripts/fmt.sh all

fmt-staged:
	$(DCD_EXEC_T) bash /workspace/scripts/fmt.sh staged

lint:
	$(DCD_EXEC_T) bash /workspace/scripts/lint.sh all

lint-staged:
	$(DCD_EXEC_T) bash /workspace/scripts/lint.sh staged

hooks:
	@git config core.hooksPath .githooks
	@chmod +x .githooks/pre-commit
	@chmod +x scripts/fmt.sh scripts/lint.sh scripts/coverage.sh
