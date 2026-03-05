# CLI Commands

All commands accept `DATABASE_URL` as an environment variable or `--connection-string` / `--db` as a flag.

## Core Migration

### `run`

Execute all phases: pre-migration scripts, declarative schema migration, post-migration scripts.

```bash
npx @mabulu-inc/schema-flow run
```

Run individual phases:

```bash
npx @mabulu-inc/schema-flow run pre      # pre-migration scripts only
npx @mabulu-inc/schema-flow run migrate  # declarative migration only
npx @mabulu-inc/schema-flow run post     # post-migration scripts only
```

### `plan`

Preview what SQL would run without applying. Alias for `run --dry-run`.

```bash
npx @mabulu-inc/schema-flow plan
```

### `validate`

Execute the full migration plan inside a transaction that always rolls back. Catches syntax errors and constraint violations without making changes.

```bash
npx @mabulu-inc/schema-flow validate
```

## Database Analysis

### `drift`

Compare YAML definitions against the live database. Reports missing columns, extra indexes, expression mismatches, and more.

```bash
npx @mabulu-inc/schema-flow drift
npx @mabulu-inc/schema-flow drift --json    # machine-readable output
npx @mabulu-inc/schema-flow drift --quiet   # exit code only
```

### `lint`

Static analysis of the migration plan for dangerous patterns.

```bash
npx @mabulu-inc/schema-flow lint
npx @mabulu-inc/schema-flow lint --json
```

### `sql`

Generate a SQL file from the migration plan without executing it.

```bash
npx @mabulu-inc/schema-flow sql
npx @mabulu-inc/schema-flow sql -o migration.sql
npx @mabulu-inc/schema-flow sql --output-dir ./migrations --name deploy
```

### `erd`

Generate a Mermaid ER diagram from your YAML schemas.

```bash
npx @mabulu-inc/schema-flow erd              # stdout
npx @mabulu-inc/schema-flow erd -o schema.md # write to file
```

### `status`

Show migration status: counts of new, changed, and unchanged files across all categories.

```bash
npx @mabulu-inc/schema-flow status
```

## Migration Control

### `down`

Show or apply a reverse migration (rollback the last run).

```bash
npx @mabulu-inc/schema-flow down              # preview rollback SQL
npx @mabulu-inc/schema-flow down --apply      # execute rollback
```

### `contract`

Finalize an expand/contract operation: drop old columns and dual-write triggers.

```bash
npx @mabulu-inc/schema-flow contract
```

### `expand-status`

Show the state of any active expand/contract column rename operations.

```bash
npx @mabulu-inc/schema-flow expand-status
```

## Scaffolding

### `init`

Create the `schema/` directory structure.

```bash
npx @mabulu-inc/schema-flow init
```

### `generate`

Generate YAML schema files from an existing database.

```bash
npx @mabulu-inc/schema-flow generate
npx @mabulu-inc/schema-flow generate --seeds users,roles  # include seed data
```

### `baseline`

Mark the current database state as the baseline. Records all schema files as applied without executing any SQL.

```bash
npx @mabulu-inc/schema-flow baseline
```

### `new`

Scaffold new files:

```bash
npx @mabulu-inc/schema-flow new pre backfill_emails    # pre-migration script
npx @mabulu-inc/schema-flow new post seed_roles        # post-migration script
npx @mabulu-inc/schema-flow new mixin soft_delete      # reusable mixin
```

### `docs`

Print the built-in YAML format reference.

```bash
npx @mabulu-inc/schema-flow docs
```

## Global Options

| Flag | Description |
|------|-------------|
| `--dry-run`, `--plan`, `-n` | Preview without applying |
| `--allow-destructive` | Allow column drops, type narrowing, table drops |
| `--lock-timeout <duration>` | Lock timeout for DDL (default: `5s`, `0` to disable) |
| `--statement-timeout <duration>` | Statement timeout for DDL (default: `30s`, `0` to disable) |
| `--max-retries <n>` | Max retries on transient errors (default: `3`) |
| `--verbose`, `-v` | Debug logging |
| `--quiet`, `-q` | Suppress non-essential output |
| `--json` | JSON output (drift, lint) |
| `--env <name>` | Select environment from config file |
| `--connection-string`, `--db` | PostgreSQL connection string |
| `--dir <path>` | Base directory (default: current directory) |
| `--schema <name>` | PostgreSQL schema (default: `public`) |
| `--skip-checks` | Skip pre-migration checks |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `SCHEMA_FLOW_DATABASE_URL` | Connection string (highest priority) |
| `DATABASE_URL` | Connection string (fallback) |
| `SCHEMA_FLOW_ALLOW_DESTRUCTIVE` | Set to `true` to allow destructive ops |
| `SCHEMA_FLOW_LOCK_TIMEOUT` | Lock timeout (default: `5s`) |
| `SCHEMA_FLOW_STATEMENT_TIMEOUT` | Statement timeout (default: `30s`) |
| `SCHEMA_FLOW_MAX_RETRIES` | Max retries (default: `3`) |
