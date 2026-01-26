.PHONY: dev dev-build dev-down dev-shell build build-local test test-unit test-integration fmt fmt-staged lint lint-staged hooks clean
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
	$(DCD_EXEC) bash -lc '/go/bin/air -c /workspace/.air.toml'

dev-dlv:
	$(DCD_EXEC) bash -lc 'bash /workspace/scripts/dlv.sh code'

dev-dlv-test:
	$(DCD_EXEC) bash -lc 'bash /workspace/scripts/dlv.sh unit'

dev-dlv-test-integration:
	$(DCD_EXEC) bash -lc 'bash /workspace/scripts/dlv.sh integration'

dev-dlv-stop:
	$(DCD_EXEC) bash -lc "pkill -f '/go/bin/dlv' || true"

test-unit:
	$(DCD_EXEC) go test -v ./...

test-integration:
	$(DCD_EXEC) go test -v -tags=integration ./tests/integration/...

build:
	$(DCD_EXEC) go build -o bin/pfs ./cmd/pfs

fmt:
	$(DCD_EXEC_T) bash -lc 'bash /workspace/scripts/fmt.sh all'

fmt-staged:
	$(DCD_EXEC_T) bash -lc 'bash /workspace/scripts/fmt.sh staged'

lint:
	$(DCD_EXEC_T) bash -lc 'bash /workspace/scripts/lint.sh all'

lint-staged:
	$(DCD_EXEC_T) bash -lc 'bash /workspace/scripts/lint.sh staged'


hooks:
	@git config core.hooksPath .githooks
	@chmod +x .githooks/pre-commit
	@chmod +x scripts/fmt.sh scripts/lint.sh
