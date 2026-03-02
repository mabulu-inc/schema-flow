#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/test-db-env.sh"

# If TEST_DATABASE_URL is set, the user has their own database — skip container start
if [ -n "${TEST_DATABASE_URL:-}" ]; then
  echo "TEST_DATABASE_URL is set — skipping container start"
  exit 0
fi

# Pick a free port: try 5432 first, otherwise let the OS assign one
pick_port() {
  if ! node -e "
    const s = require('net').createServer();
    s.once('error', () => process.exit(1));
    s.listen(5432, () => { s.close(); process.exit(0) });
  " 2>/dev/null; then
    # 5432 is taken — grab a random free port from the OS
    node -e "
      const s = require('net').createServer();
      s.listen(0, () => { console.log(s.address().port); s.close() });
    "
  else
    echo 5432
  fi
}

PORT=$(pick_port)
echo "$PORT" > "$PORT_FILE"

$CONTAINER_RUNTIME run -d \
  --name "$CONTAINER_NAME" \
  --tmpfs /var/lib/postgresql/data \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=postgres \
  -p "$PORT":5432 \
  postgres:latest \
  -c fsync=off \
  -c full_page_writes=off \
  -c synchronous_commit=off

echo "Waiting for Postgres on port $PORT..."
until $CONTAINER_RUNTIME exec "$CONTAINER_NAME" pg_isready -U postgres >/dev/null 2>&1; do
  sleep 1
done
echo "Postgres ready on port $PORT (in-memory mode)"
