#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/test-db-env.sh"

$CONTAINER_RUNTIME rm -f "$CONTAINER_NAME" 2>/dev/null || true
rm -f "$PORT_FILE"
