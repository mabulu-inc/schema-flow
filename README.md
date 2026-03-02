# @mabulu-inc/schema-flow

**Declarative zero-downtime PostgreSQL migrations.**

Schema-flow takes your database where it needs to be. Define the desired end state in YAML, and it figures out what changes are required. Pre- and post-migration SQL scripts handle everything else — column renames, data backfills, seed data, grants.

```
pnpm exec schema-flow plan     # see what would change
pnpm exec schema-flow run      # apply pre → migrate → post
```

## Why schema-flow?

- **Declarative tables** — Define the desired end state in YAML. Schema-flow figures out the diff.
- **Zero new dependencies for your team** — Just `pnpm add -D @mabulu-inc/schema-flow` and go. No Java, no Go binary, no Docker requirement.
- **Convention over configuration** — Drop files in `schema/`, `pre/`, `post/`. That's it.
- **Surgical change detection** — filepath + SHA-256 hash tracking means only new or changed files are processed.
- **Safe FK ordering** — Tables are created first (without foreign keys), then all FKs are added after every table exists.
- **CI/CD ready** — Non-zero exit codes on failure, structured logging, `--quiet` mode for pipelines.
- **Plan before apply** — `schema-flow plan` shows exactly what SQL would run, without touching the database.
- **Phase control** — Run `pre`, `migrate`, `post` independently or all at once.
- **Generate from existing DB** — Bootstrap your schema files from a live database with `schema-flow generate`.

## Install

```bash
pnpm add -D @mabulu-inc/schema-flow

# or
npm install --save-dev @mabulu-inc/schema-flow
```

## Quick Start

### 1. Initialize the directory structure

```bash
pnpm exec schema-flow init
```

This creates:

```
schema/     ← Declarative YAML table definitions (one file per table)
pre/        ← Pre-migration SQL scripts (run before schema changes)
post/       ← Post-migration SQL scripts (run after schema changes)
```

### 2. Define your tables

**schema/users.yaml**

```yaml
table: users

columns:
  - name: id
    type: serial
    primary_key: true

  - name: email
    type: varchar(255)
    unique: true

  - name: display_name
    type: varchar(100)

  - name: is_active
    type: boolean
    default: "true"

  - name: created_at
    type: timestamptz
    default: now()
```

**schema/posts.yaml**

```yaml
table: posts

columns:
  - name: id
    type: serial
    primary_key: true

  - name: author_id
    type: integer
    references:
      table: users
      column: id
      on_delete: CASCADE

  - name: title
    type: varchar(255)

  - name: body
    type: text
    nullable: true

  - name: created_at
    type: timestamptz
    default: now()
```

### 3. Set your database URL

```bash
export DATABASE_URL="postgresql://user:pass@localhost:5432/mydb"
```

### 4. Preview changes

```bash
pnpm exec schema-flow plan
```

### 5. Apply

```bash
pnpm exec schema-flow run
```

## Commands

| Command | Description |
| --- | --- |
| `schema-flow init` | Create `schema/`, `pre/`, `post/` directories |
| `schema-flow plan` | Show what would be done (dry run) |
| `schema-flow run` | Run all phases: pre → migrate → post |
| `schema-flow run pre` | Run only pre-migration scripts |
| `schema-flow run migrate` | Run only declarative schema migration |
| `schema-flow run post` | Run only post-migration scripts |
| `schema-flow status` | Show pending changes |
| `schema-flow generate` | Generate YAML schema files from an existing database |
| `schema-flow new pre <name>` | Scaffold a timestamped pre-migration script |
| `schema-flow new post <name>` | Scaffold a timestamped post-migration script |
| `schema-flow help` | Show help |

## Options

| Flag | Description |
| --- | --- |
| `--dry-run`, `--plan`, `-n` | Preview without applying |
| `--verbose`, `-v` | Debug-level logging |
| `--quiet`, `-q` | Suppress non-essential output |
| `--connection-string`, `--db` | PostgreSQL connection string |
| `--dir` | Base directory (default: cwd) |
| `--schema` | PostgreSQL schema (default: `public`) |

## Schema YAML Reference

Each table gets its own file. Everything about the table — columns, constraints, foreign keys, indexes, checks — lives in that one file.

### Column Properties

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `name` | string | required | Column name |
| `type` | string | required | PostgreSQL type (`serial`, `varchar(255)`, `integer`, `text`, `boolean`, `timestamptz`, `jsonb`, etc.) |
| `nullable` | boolean | `false` | Whether the column allows NULL. Columns are NOT NULL by default. |
| `default` | string | — | SQL default expression (e.g., `now()`, `'active'`, `true`) |
| `primary_key` | boolean | `false` | Single-column primary key |
| `unique` | boolean | `false` | Unique constraint |
| `references` | object | — | Foreign key definition |

### Foreign Key Properties

```yaml
references:
  table: other_table     # Referenced table
  column: id             # Referenced column
  on_delete: CASCADE     # NO ACTION | RESTRICT | CASCADE | SET NULL | SET DEFAULT
  on_update: NO ACTION   # NO ACTION | RESTRICT | CASCADE | SET NULL | SET DEFAULT
```

### Composite Primary Key

```yaml
primary_key: [post_id, tag_id]
```

### Indexes

```yaml
indexes:
  - columns: [email]
    unique: true
  - columns: [status, created_at]
  - name: idx_active_users
    columns: [is_active]
    where: "is_active = true"    # Partial index
```

### Check Constraints

```yaml
checks:
  - name: chk_positive_amount
    expression: "amount > 0"
```

## Pre/Post Migration Scripts

For operations that can't be expressed declaratively — column renames, data migrations, grants, seed data — use SQL scripts.

### Scaffold a script

```bash
pnpm exec schema-flow new pre rename_user_email
# → pre/20260228153000_rename_user_email.sql

pnpm exec schema-flow new post seed_roles
# → post/20260228153005_seed_roles.sql
```

Scripts are executed in alphabetical order. The UTC timestamp prefix ensures correct ordering.

### Example pre-migration script

```sql
-- pre/20260228153000_rename_user_email.sql
BEGIN;
ALTER TABLE "public"."users" RENAME COLUMN "name" TO "display_name";
COMMIT;
```

### Example post-migration script

```sql
-- post/20260228160000_seed_roles.sql
BEGIN;
INSERT INTO "public"."roles" (name) VALUES ('admin'), ('editor'), ('viewer')
ON CONFLICT (name) DO NOTHING;
COMMIT;
```

## Generate from Existing Database

Bootstrap your schema files from a live database:

```bash
pnpm exec schema-flow generate --db "postgresql://user:pass@localhost:5432/mydb"
```

This introspects every table and function in the `public` schema and writes a YAML file for each one into `schema/`.

## Change Tracking

Schema-flow stores a filepath + SHA-256 hash in a `_schema_flow_history` table in your database. On each run, it compares every file's current hash against the stored hash:

- **New file** → process it
- **Changed hash** → process it
- **Same hash** → skip it

This makes repeated runs safe and fast.

## How FK Ordering Works

Because each table is in its own file but may reference other tables via foreign keys, schema-flow uses a two-phase approach:

1. **Structure phase** — `CREATE TABLE` statements (without FKs), `ALTER TABLE` for column changes, indexes
2. **Foreign key phase** — `ALTER TABLE ... ADD CONSTRAINT` for all foreign keys

This ensures tables exist before they're referenced, regardless of file order.

## CI/CD Integration

```yaml
# GitHub Actions example
- name: Run database migrations
  run: pnpm exec schema-flow run --quiet
  env:
    DATABASE_URL: ${{ secrets.DATABASE_URL }}
```

Schema-flow exits with code `1` on any failure, making it safe for pipelines. Use `--quiet` to suppress non-essential output, or `--verbose` for full debug logs.

### Pipeline-safe dry run

```yaml
- name: Preview migrations
  run: pnpm exec schema-flow plan --quiet
```

## Environment Variables

| Variable | Description |
| --- | --- |
| `DATABASE_URL` | PostgreSQL connection string |
| `SCHEMA_FLOW_DATABASE_URL` | Alternative connection string (takes precedence) |
| `SCHEMA_FLOW_LOG_LEVEL` | `debug`, `info`, `warn`, `error`, `silent` |

## Programmatic API

```typescript
import { resolveConfig, runAll, buildPlan, closePool } from "@mabulu-inc/schema-flow";

const config = resolveConfig({
  connectionString: process.env.DATABASE_URL,
  dryRun: true,
});

const results = await runAll(config);
console.log(results);

await closePool();
```

## License

MIT — see [LICENSE](LICENSE).
