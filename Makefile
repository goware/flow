SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help
MAKEFLAGS += --no-print-directory

GO         ?= go
PACKAGES   ?= ./...
TEST       ?=
# Keep database-backed packages and parallel tests within a typical local
# PostgreSQL connection budget. -count=1 prevents stale external-state results.
TEST_FLAGS ?= -count=1 -p 1 -parallel 4

PG_HOST           ?= 127.0.0.1
PG_PORT           ?= 5432
PG_USER           ?= postgres
PGPASSWORD        ?= postgres
PG_DATABASE       ?= flow_test
PG_ADMIN_DATABASE ?= postgres
PG_SSLMODE        ?= disable

FLOW_TEST_ADMIN_DATABASE ?= $(PG_ADMIN_DATABASE)

ifeq ($(origin FLOW_TEST_DATABASE_URL), undefined)
FLOW_TEST_DATABASE_URL := postgres://$(PG_USER)@$(PG_HOST):$(PG_PORT)/$(PG_DATABASE)?sslmode=$(PG_SSLMODE)
FLOW_TEST_DATABASE_PASSWORD ?= $(PGPASSWORD)
endif

export FLOW_TEST_DATABASE_URL
export FLOW_TEST_DATABASE_PASSWORD
export FLOW_TEST_ADMIN_DATABASE

.PHONY: help build test test-with-reset db-reset db-migrate db-status

help:
	@echo "Flow development targets"
	@echo
	@echo "  make build            Compile all packages and examples"
	@echo "  make test             Run the complete suite with the race detector"
	@echo "  make test-with-reset  Reset flow_test, then run the complete suite"
	@echo
	@echo "  make db-reset         Recreate flow_test and apply embedded migrations"
	@echo "  make db-migrate       Apply pending migrations without resetting data"
	@echo "  make db-status        Verify and print the Flow schema version"
	@echo
	@echo "Override PG_HOST, PG_PORT, PG_USER, PG_DATABASE, PGPASSWORD, or"
	@echo "FLOW_TEST_DATABASE_URL as needed. Use TEST=<regexp> to select tests."

build:
	$(GO) build $(PACKAGES)

test:
	$(GO) test -race $(TEST_FLAGS) $(if $(TEST),-run=$(TEST)) $(PACKAGES)

test-with-reset: db-reset
	$(MAKE) test

db-reset:
	$(GO) run ./tools/db reset

db-migrate:
	$(GO) run ./tools/db migrate

db-status:
	$(GO) run ./tools/db status
