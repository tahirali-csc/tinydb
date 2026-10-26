.PHONY: build test run clean help

CARGO ?= cargo
DB ?= tinydb.data

help:
	@echo "TinyDB development commands:"
	@echo "  make build       Compile TinyDB"
	@echo "  make test        Run TinyDB unit tests"
	@echo "  make run         Run TinyDB using $(DB)"
	@echo "  make clean       Remove Rust build artifacts"

build:
	$(CARGO) build

test:
	$(CARGO) test

run:
	$(CARGO) run -- $(DB)

clean:
	$(CARGO) clean
