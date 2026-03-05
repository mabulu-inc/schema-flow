# Multi-Schema

An analytics platform with two PostgreSQL schemas — demonstrates **cross-schema foreign keys**.

## Architecture

```
core schema                    analytics schema
┌──────────────┐              ┌───────────────────┐
│ teams        │◄─────────────│ dashboards        │
│   id PK      │   FK         │   team_id → core  │
│   name       │              │   owner_id → core │
└──────────────┘              └───────────────────┘
                                       ▲
┌──────────────┐              ┌────────┴──────────┐
│ users        │◄─────────────│ events            │
│   id PK      │   FK         │   user_id → core  │
│   email      │              │   event_type      │
│   team_id FK │              │   payload (jsonb)  │
└──────────────┘              └───────────────────┘
                              ┌───────────────────┐
                              │ recent_events     │
                              │   (view, joins    │
                              │    across schemas) │
                              └───────────────────┘
```

## What this example covers

| Feature              | Where                                        |
| -------------------- | -------------------------------------------- |
| Multiple schemas     | `core/schema/`, `analytics/schema/`          |
| Cross-schema FKs     | `events.user_id → core.users`                |
| Cross-schema views   | `recent_events` joins `analytics.events` with `core.users` |
| Migration ordering   | Core runs first, analytics second             |

## Key concept: `references.schema`

When a table in one schema needs a foreign key to a table in another schema, add `schema` to the reference:

```yaml
# analytics/schema/tables/events.yaml
columns:
  - name: user_id
    type: integer
    references:
      schema: core          # ← cross-schema reference
      table: users
      column: id
```

This generates: `REFERENCES "core"."users" ("id")` instead of the default `REFERENCES "analytics"."users"`.

## Usage

Schemas must be migrated **in dependency order** — core first, then analytics:

```bash
# Create schemas (one-time)
psql $DATABASE_URL -c "CREATE SCHEMA IF NOT EXISTS core"
psql $DATABASE_URL -c "CREATE SCHEMA IF NOT EXISTS analytics"

# Migrate core first
npx schema-flow run --dir examples/multi-schema/core --schema core

# Then analytics (depends on core)
npx schema-flow run --dir examples/multi-schema/analytics --schema analytics
```

Or use environments in `schema-flow.config.yaml`:

```yaml
environments:
  core:
    connectionString: ${DATABASE_URL}
    pgSchema: core
  analytics:
    connectionString: ${DATABASE_URL}
    pgSchema: analytics
```

```bash
npx schema-flow run --env core
npx schema-flow run --env analytics
```
