PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# DuckDB version to build against
DUCKDB_VERSION ?= v1.4.4

# Parallel build (cmake respects this env var)
export CMAKE_BUILD_PARALLEL_LEVEL ?= $(shell sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4)

# OpenSSL detection (required for HTTPS support)
# macOS: Homebrew installs to /opt/homebrew/opt/openssl (ARM) or /usr/local/opt/openssl (Intel)
# Linux: usually system-installed at /usr, or /usr/local
OPENSSL_ROOT_DIR ?= $(shell \
	if [ -d "/opt/homebrew/opt/openssl" ]; then echo "/opt/homebrew/opt/openssl"; \
	elif [ -d "/usr/local/opt/openssl" ]; then echo "/usr/local/opt/openssl"; \
	elif [ -d "/usr/local/opt/openssl@3" ]; then echo "/usr/local/opt/openssl@3"; \
	elif [ -d "/opt/homebrew/opt/openssl@3" ]; then echo "/opt/homebrew/opt/openssl@3"; \
	elif pkg-config --exists openssl 2>/dev/null; then pkg-config --variable=prefix openssl; \
	elif [ -f "/usr/include/openssl/ssl.h" ]; then echo "/usr"; \
	elif [ -f "/usr/local/include/openssl/ssl.h" ]; then echo "/usr/local"; \
	else echo ""; \
	fi)

ifneq ($(OPENSSL_ROOT_DIR),)
	EXT_FLAGS += -DOPENSSL_ROOT_DIR=$(OPENSSL_ROOT_DIR)
endif

# Configuration of extension
EXT_NAME=inferal_relay
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Auto-clone DuckDB and extension-ci-tools if not present
duckdb/.git:
	git clone --depth=1 --branch $(DUCKDB_VERSION) https://github.com/duckdb/duckdb.git duckdb

extension-ci-tools/.git:
	git clone --depth=1 https://github.com/duckdb/extension-ci-tools.git extension-ci-tools

.PHONY: deps
deps: duckdb/.git extension-ci-tools/.git

# Include the Makefile from extension-ci-tools (must exist before make parses it)
# We use a conditional include so the first `make deps` works
-include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Override release/debug to depend on deps
release: deps
debug: deps
test: deps

# Developer workflow targets
DB ?=

.PHONY: duckdb
duckdb: release
ifdef DB
	./build/release/duckdb $(DB) -unsigned -cmd "LOAD 'build/release/extension/inferal_relay/inferal_relay.duckdb_extension';"
else
	./build/release/duckdb -unsigned -cmd "LOAD 'build/release/extension/inferal_relay/inferal_relay.duckdb_extension';"
endif

.PHONY: clean-deps
clean-deps:
	rm -rf duckdb extension-ci-tools
