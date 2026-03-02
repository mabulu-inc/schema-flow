#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/test-db-env.sh"

# If TEST_DATABASE_URL is set, no container was started — nothing to stop
if [ -n "${TEST_DATABASE_URL:-}" ]; then
  exit 0
fi

$CONTAINER_RUNTIME rm -f "$CONTAINER_NAME" 2>/dev/null || true
rm -f "$PORT_FILE"
