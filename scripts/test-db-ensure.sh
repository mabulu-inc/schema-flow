#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/test-db-env.sh"

# If the container is already running and healthy, nothing to do
if $CONTAINER_RUNTIME exec "$CONTAINER_NAME" pg_isready -U postgres >/dev/null 2>&1; then
  exit 0
fi

# Otherwise, remove any stale container and start fresh
$CONTAINER_RUNTIME rm -f "$CONTAINER_NAME" 2>/dev/null || true
exec "$(dirname "$0")/test-db-start.sh"
