#!/usr/bin/env bash
# Shared config for test database scripts.
# Source this file — do not execute directly.

CONTAINER_NAME="sf-postgres"
CONTAINER_RUNTIME="${CONTAINER_RUNTIME:-$(command -v podman >/dev/null 2>&1 && echo podman || echo docker)}"
PORT_FILE="$(cd "$(dirname "$0")/.." && pwd)/.test-db-port"
