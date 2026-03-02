# @mabulu-inc/schema-flow

**Declarative zero-downtime PostgreSQL migrations.**

Schema-flow takes your database where it needs to be. Define the desired end state in YAML, and it figures out what changes are required. Pre- and post-migration SQL scripts handle everything else — column renames, data backfills, seed data, grants.

```
npx @mabulu-inc/schema-flow plan     # see what would change
npx @mabulu-inc/schema-flow run      # apply pre → migrate → post
```

## Why schema-flow?

- **Declarative tables** — Define the desired end state in YAML. Schema-flow figures out the diff.
- **Safe by default** — Only additive operations run unless you explicitly opt in to destructive changes with `--allow-destructive`.
- **Zero new dependencies for your team** — Run via `npx` (or `pnpx`). No install step, no Java, no Go binary, no Docker requirement.
- **Convention over configuration** — Drop files in `schema-flow/schema/`, `schema-flow/pre/`, `schema-flow/post/`. That's it.
- **Surgical change detection** — filepath + SHA-256 hash tracking means only new or changed files are processed.
- **Safe FK ordering** — Tables are created first (without foreign keys), then all FKs are added after every table exists.
- **CI/CD ready** — Non-zero exit codes on failure, structured logging, `--quiet` mode for pipelines.
- **Plan before apply** — `schema-flow plan` shows exactly what SQL would run, without touching the database.
- **Phase control** — Run `pre`, `migrate`, `post` independently or all at once.
- **Generate from existing DB** — Bootstrap your schema files from a live database with `schema-flow generate`.

## Quick Start

### 1. Initialize the directory structure

```bash
npx @mabulu-inc/schema-flow init
```

This creates:

```
schema-flow/
  schema/     ← Declarative YAML table definitions (one file per table) and function files (fn_*.yaml)
  pre/        ← Pre-migration SQL scripts (run before schema changes)
  post/       ← Post-migration SQL scripts (run after schema changes)
  mixins/     ← Reusable schema mixins (timestamps, soft_delete, etc.)
```

### 2. Define your tables

**schema-flow/schema/users.yaml**

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

**schema-flow/schema/posts.yaml**

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
npx @mabulu-inc/schema-flow plan
```

### 5. Apply

```bash
npx @mabulu-inc/schema-flow run
```

## Commands

| Command | Description |
| --- | --- |
| `schema-flow init` | Create `schema-flow/` directory with `schema/`, `pre/`, `post/` subdirectories |
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

All commands are invoked via `npx` (or `pnpx`):

```bash
npx @mabulu-inc/schema-flow <command> [options]
```

## Options

| Flag | Description |
| --- | --- |
| `--dry-run`, `--plan`, `-n` | Preview without applying |
| `--allow-destructive` | Allow destructive operations (column drops, type narrowing, SET NOT NULL) |
| `--verbose`, `-v` | Debug-level logging |
| `--quiet`, `-q` | Suppress non-essential output |
| `--connection-string`, `--db` | PostgreSQL connection string |
| `--dir` | Base directory (default: cwd) |
| `--schema` | PostgreSQL schema (default: `public`) |
| `-h`, `--help` | Show help |
| `-V`, `--version` | Show version |

## Safe by Default

Schema-flow is designed for zero-downtime deployments. By default, it only performs safe, additive operations:

| Operation | Safe | Requires `--allow-destructive` |
| --- | --- | --- |
| Create table | ✓ | |
| Add column | ✓ | |
| Add index (CONCURRENTLY) | ✓ | |
| Add foreign key | ✓ | |
| Add check constraint | ✓ | |
| Widen type (int → bigint) | ✓ | |
| Make column nullable | ✓ | |
| Set / change default | ✓ | |
| Create trigger | ✓ | |
| Replace trigger (changed definition) | ✓ | |
| Drop trigger (removed from YAML) | | ✓ |
| Drop column | | ✓ |
| Narrow type (bigint → int) | | ✓ |
| Make column NOT NULL | | ✓ |

If a column exists in the database but is missing from the schema YAML, schema-flow will **not** drop it unless `--allow-destructive` is set. Instead, it logs a warning and skips the drop.

Use `schema-flow plan` to preview which operations would be applied and which would be blocked:

```bash
# See what's safe and what's blocked
npx @mabulu-inc/schema-flow plan

# See everything, including destructive operations
npx @mabulu-inc/schema-flow plan --allow-destructive

# Apply only safe changes
npx @mabulu-inc/schema-flow run

# Apply everything (use with caution)
npx @mabulu-inc/schema-flow run --allow-destructive
```

For column renames and other inherently destructive operations, use a procedural pre-migration script instead of modifying the YAML.

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

### Triggers

Declare triggers directly on tables. The referenced function must exist (define it in a `fn_*.yaml` file).

```yaml
triggers:
  - name: set_orders_updated_at
    timing: BEFORE           # BEFORE | AFTER | INSTEAD OF
    events: [UPDATE]         # INSERT, UPDATE, DELETE, TRUNCATE
    function: update_timestamp
    for_each: ROW            # ROW | STATEMENT (default: ROW)
    when: "OLD.* IS DISTINCT FROM NEW.*"  # optional WHEN clause
```

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `name` | string | required | Trigger name |
| `timing` | string | required | `BEFORE`, `AFTER`, or `INSTEAD OF` |
| `events` | string[] | required | Array of `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE` |
| `function` | string | required | Name of the trigger function to execute |
| `for_each` | string | `ROW` | `ROW` or `STATEMENT` |
| `when` | string | — | Optional SQL condition |

### Mixins

Mixins let you DRY up repeated schema patterns (timestamps, soft delete, tenant scoping, etc.). Define a mixin once in `schema-flow/mixins/`, then apply it to any table with `use:`.

**schema-flow/mixins/timestamps.yaml**

```yaml
mixin: timestamps

columns:
  - name: created_at
    type: timestamptz
    default: now()
  - name: updated_at
    type: timestamptz
    default: now()

triggers:
  - name: set_{table}_updated_at
    timing: BEFORE
    events: [UPDATE]
    function: update_timestamp
    for_each: ROW
```

**Using a mixin in a table:**

```yaml
table: users
use: [timestamps]

columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: varchar(255)
```

The `{table}` placeholder in trigger, index, and check `name` fields is replaced with the table name during expansion. In the example above, the trigger becomes `set_users_updated_at`.

**Merge rules:**

- **Columns**: mixin columns are prepended (in `use` order), then table columns follow. If a table defines a column with the same name as a mixin column, the table's definition wins.
- **Indexes, checks, triggers**: mixin entries are added before table entries.
- The `use` property is stripped before planning — it's purely a composition mechanism.

### Function Schema

Functions use separate files with the `fn_` prefix (e.g., `fn_update_timestamp.yaml`).

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `name` | string | required | Function name |
| `language` | string | `plpgsql` | SQL language (`plpgsql`, `sql`, etc.) |
| `returns` | string | `void` | Return type |
| `args` | string | — | Argument list (e.g., `p_id integer, p_name text`) |
| `body` | string | required | Function body |
| `replace` | boolean | `true` | Use `CREATE OR REPLACE` |

```yaml
# schema-flow/schema/fn_update_timestamp.yaml
name: update_timestamp
language: plpgsql
returns: trigger
body: |
  BEGIN
    NEW.updated_at = now();
    RETURN NEW;
  END;
```

Function files (prefixed with `fn_`) are automatically detected and applied with `CREATE OR REPLACE FUNCTION` before schema migration runs. This ensures trigger functions exist before any triggers reference them.

## Pre/Post Migration Scripts

For operations that can't be expressed declaratively — column renames, data migrations, grants, seed data — use SQL scripts.

### Scaffold a script

```bash
npx @mabulu-inc/schema-flow new pre rename_user_email
# → schema-flow/pre/20260228153000_rename_user_email.sql

npx @mabulu-inc/schema-flow new post seed_roles
# → schema-flow/post/20260228153005_seed_roles.sql
```

Scripts are executed in alphabetical order. The UTC timestamp prefix ensures correct ordering.

### Example pre-migration script

```sql
-- schema-flow/pre/20260228153000_rename_user_email.sql
BEGIN;
ALTER TABLE "public"."users" RENAME COLUMN "name" TO "display_name";
COMMIT;
```

### Example post-migration script

```sql
-- schema-flow/post/20260228160000_seed_roles.sql
BEGIN;
INSERT INTO "public"."roles" (name) VALUES ('admin'), ('editor'), ('viewer')
ON CONFLICT (name) DO NOTHING;
COMMIT;
```

## Generate from Existing Database

Bootstrap your schema files from a live database:

```bash
npx @mabulu-inc/schema-flow generate --db "postgresql://user:pass@localhost:5432/mydb"
```

This introspects every table and function in the `public` schema and writes a YAML file for each one into `schema-flow/schema/`.

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
  run: npx @mabulu-inc/schema-flow run --quiet
  env:
    DATABASE_URL: ${{ secrets.DATABASE_URL }}
```

Schema-flow exits with code `1` on any failure, making it safe for pipelines. Use `--quiet` to suppress non-essential output, or `--verbose` for full debug logs.

### Pipeline-safe dry run

```yaml
- name: Preview migrations
  run: npx @mabulu-inc/schema-flow plan --quiet
```

## Directory Structure

schema-flow looks for a `schema-flow/` directory in the current working directory (or the directory specified with `--dir`):

```
my-project/
  schema-flow/
    schema/         ← One YAML file per table or function
      users.yaml
      posts.yaml
      fn_update_timestamp.yaml
    mixins/         ← Reusable schema mixins
      timestamps.yaml
      soft_delete.yaml
    pre/            ← Pre-migration SQL scripts
      20260228153000_rename_column.sql
    post/           ← Post-migration SQL scripts
      20260228160000_seed_roles.sql
  src/
  package.json
```

Run all commands from your project root — schema-flow automatically finds the `schema-flow/` folder.

## Environment Variables

| Variable | Description |
| --- | --- |
| `DATABASE_URL` | PostgreSQL connection string |
| `SCHEMA_FLOW_DATABASE_URL` | Alternative connection string (takes precedence) |
| `SCHEMA_FLOW_LOG_LEVEL` | `debug`, `info`, `warn`, `error`, `silent` |
| `SCHEMA_FLOW_ALLOW_DESTRUCTIVE` | Set to `true` to allow destructive operations (same as `--allow-destructive`) |

## Programmatic API

```typescript
import { resolveConfig, runAll, closePool } from "@mabulu-inc/schema-flow";

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
