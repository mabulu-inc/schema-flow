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
  repeatable/ ← SQL scripts re-run whenever their content changes (grants, refresh, etc.)
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
| `schema-flow validate` | Validate schema against a live database (executes in a transaction, always rolls back) |
| `schema-flow run` | Run all phases: pre → migrate → post |
| `schema-flow run pre` | Run only pre-migration scripts |
| `schema-flow run migrate` | Run only declarative schema migration |
| `schema-flow run post` | Run only post-migration scripts |
| `schema-flow drift` | Compare live database vs. YAML and report differences |
| `schema-flow lint` | Static analysis of migration plan for dangerous patterns |
| `schema-flow down` | Show reverse migration plan (rollback last run) |
| `schema-flow down --apply` | Execute reverse migration |
| `schema-flow sql` | Generate migration SQL file from plan |
| `schema-flow erd` | Generate Mermaid ER diagram from schema YAML |
| `schema-flow contract` | Finalize expand/contract: drop old columns and triggers |
| `schema-flow expand-status` | Show current expand/contract operation status |
| `schema-flow status` | Show pending changes |
| `schema-flow baseline` | Mark existing database as managed without running migrations |
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
| `--allow-destructive` | Allow destructive operations (column drops, type narrowing, disable RLS, drop policies) |
| `--lock-timeout <duration>` | Lock timeout for DDL statements (default: `5s`, `0` to disable) |
| `--statement-timeout <duration>` | Statement timeout for DDL statements (default: `30s`, `0` to disable) |
| `--verbose`, `-v` | Debug-level logging |
| `--quiet`, `-q` | Suppress non-essential output |
| `--json` | Output in JSON format (`drift`, `lint`) |
| `--output`, `-o <file>` | Write output to a file (`erd`, `sql`) |
| `--output-dir <dir>` | Output directory (`sql`) |
| `--name <name>` | Output file name suffix (`sql`) |
| `--skip-checks` | Skip pre-migration checks |
| `--apply` | Execute the operation (`down`) |
| `--env <name>` | Select environment from config file |
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
| Add foreign key (NOT VALID + VALIDATE) | ✓ | |
| Add check constraint (NOT VALID + VALIDATE) | ✓ | |
| Add unique constraint (concurrent index) | ✓ | |
| Widen type (int → bigint) | ✓ | |
| Make column nullable | ✓ | |
| Make column NOT NULL (safe 4-step) | ✓ | |
| Set / change default | ✓ | |
| Create trigger | ✓ | |
| Replace trigger (changed definition) | ✓ | |
| Enable RLS | ✓ | |
| Force RLS | ✓ | |
| Create policy | ✓ | |
| Replace policy (changed definition) | ✓ | |
| Create enum | ✓ | |
| Add enum value | ✓ | |
| Create extension | ✓ | |
| Create/replace view | ✓ | |
| Set comment (table or column) | ✓ | |
| Drop trigger (removed from YAML) | | ✓ |
| Drop policy (removed from YAML) | | ✓ |
| Disable RLS (removed from YAML) | | ✓ |
| Drop extension | | ✓ |
| Create/drop materialized view | ✓ / ✓ | |
| Drop index | | ✓ |
| Drop column | | ✓ |
| Narrow type (bigint → int) | | ✓ |

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

For zero-downtime column renames and transforms, see [Expand/Contract](#expandcontract-pattern) below. For other inherently destructive operations, use a procedural pre-migration script instead of modifying the YAML.

> **`plan` vs `validate`**: `plan` generates and displays the SQL but never sends it to PostgreSQL. `validate` executes the SQL inside a transaction that always rolls back — Postgres itself checks syntax, references, and types, catching errors that `plan` cannot.

## Zero-Downtime Safety

Schema-flow uses several techniques to avoid taking heavy locks that block queries on large tables.

### Lock Timeout

Every DDL session sets `lock_timeout` (default: `5s`). If a lock can't be acquired within this window, the statement fails fast instead of blocking all other queries while waiting.

```bash
npx @mabulu-inc/schema-flow run --lock-timeout 10s
# or via environment variable
SCHEMA_FLOW_LOCK_TIMEOUT=10s npx @mabulu-inc/schema-flow run
# disable lock timeout
npx @mabulu-inc/schema-flow run --lock-timeout 0
```

### Statement Timeout

A `statement_timeout` (default: `30s`) guards against runaway migrations. Set to `0` to disable.

```bash
npx @mabulu-inc/schema-flow run --statement-timeout 60s
SCHEMA_FLOW_STATEMENT_TIMEOUT=60s npx @mabulu-inc/schema-flow run
# disable statement timeout
npx @mabulu-inc/schema-flow run --statement-timeout 0
```

### Advisory Locking

Schema-flow acquires a PostgreSQL advisory lock (derived from the schema name) before running any non-dry-run migration. This prevents two concurrent `schema-flow run` processes from colliding. The lock is released automatically when the migration completes or fails.

### Foreign Keys — NOT VALID + VALIDATE

Foreign keys are created in two steps:

1. `ADD CONSTRAINT ... NOT VALID` — takes a brief `SHARE ROW EXCLUSIVE` lock, does not scan the table
2. `VALIDATE CONSTRAINT` — runs outside any transaction, takes only `SHARE UPDATE EXCLUSIVE` (doesn't block reads or writes)

This avoids the full-table lock that a normal `ADD CONSTRAINT ... FOREIGN KEY` would take.

### Safe NOT NULL (PG 12+)

Making a column NOT NULL traditionally requires an `ACCESS EXCLUSIVE` lock and a full table scan. Schema-flow uses a safe 4-step pattern instead:

1. `ADD CONSTRAINT chk_{table}_{col}_nn CHECK ("{col}" IS NOT NULL) NOT VALID` — instant, no scan
2. `VALIDATE CONSTRAINT chk_{table}_{col}_nn` — scans rows outside a transaction (`SHARE UPDATE EXCLUSIVE`)
3. `ALTER COLUMN SET NOT NULL` — instant when PG sees the validated check constraint (PG 12+)
4. `DROP CONSTRAINT chk_{table}_{col}_nn` — cleanup

All steps are non-destructive and safe-mode compatible. If a migration is interrupted mid-way, re-running picks up where it left off.

### Safe CHECK Constraints

CHECK constraints on existing tables use the same two-step approach: `ADD CONSTRAINT ... NOT VALID` followed by `VALIDATE CONSTRAINT`.

### Safe UNIQUE Constraints

Adding a unique constraint to an existing column avoids a table lock:

1. `CREATE UNIQUE INDEX CONCURRENTLY` — builds the index without blocking writes
2. `ADD CONSTRAINT ... UNIQUE USING INDEX` — attaches the index as a constraint (instant)

### Execution Order

```
1. Extensions               (in transaction)   ← CREATE EXTENSION IF NOT EXISTS
2. Enums                     (in transaction)   ← CREATE TYPE / ADD VALUE
3. Functions                 (in transaction)
4. Structure ops — non-index (in transaction)   ← includes ADD CONSTRAINT ... NOT VALID
5. Structure ops — indexes   (outside txn)      ← CONCURRENTLY
6. FK ops                    (in transaction)    ← NOT VALID
7. Validate ops              (outside txn)      ← VALIDATE CONSTRAINT, SET NOT NULL, cleanup
8. Views                     (in transaction)   ← CREATE OR REPLACE VIEW
9. Materialized Views        (in transaction)   ← CREATE MATERIALIZED VIEW / REFRESH
```

### Intermediate State Recovery

If a migration is interrupted after step 2 but before step 5, re-running `schema-flow run` detects the intermediate state (e.g., an unvalidated FK or existing helper check constraint) and emits only the remaining steps.

## Schema YAML Reference

Each table gets its own file. Everything about the table — columns, constraints, foreign keys, indexes, checks — lives in that one file.

### Column Properties

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `name` | string | required | Column name |
| `type` | string | required | PostgreSQL type (`serial`, `varchar(255)`, `integer`, `text`, `boolean`, `timestamptz`, `jsonb`, etc.) |
| `nullable` | boolean | `false` | Whether the column allows NULL. Columns are NOT NULL by default. |
| `default` | string | — | SQL default expression, inserted verbatim (see note below) |
| `primary_key` | boolean | `false` | Single-column primary key |
| `unique` | boolean | `false` | Unique constraint |
| `references` | object | — | Foreign key definition |
| `generated` | string | — | SQL expression for generated stored column (`GENERATED ALWAYS AS ... STORED`) |
| `comment` | string | — | Column description (`COMMENT ON COLUMN`) |

**`default` values** are inserted verbatim into the generated SQL, so SQL string literals need inner single quotes:

```yaml
default: now()          # → DEFAULT now()         (function call)
default: "true"         # → DEFAULT true          (boolean)
default: 42             # → DEFAULT 42            (number)
default: "'draft'"      # → DEFAULT 'draft'       (string literal — note the inner quotes)
```

### Foreign Key Properties

```yaml
references:
  table: other_table     # Referenced table
  column: id             # Referenced column
  on_delete: CASCADE     # NO ACTION | RESTRICT | CASCADE | SET NULL | SET DEFAULT
  on_update: NO ACTION   # NO ACTION | RESTRICT | CASCADE | SET NULL | SET DEFAULT
  deferrable: true       # Make the FK constraint deferrable
  initially_deferred: true  # Defer checking until transaction commit
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
  - columns: [metadata]
    method: gin                  # GIN, GiST, BRIN, hash (default: btree)
  - columns: [metadata]
    method: gin
    opclass: jsonb_path_ops      # Operator class
  - columns: [email]
    include: [name]              # Covering index (INCLUDE)
  - columns: ["lower(email)"]   # Expression index (not quoted)
    unique: true
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

### Row-Level Security (RLS)

Declare RLS policies directly on tables. Policies are diffed and managed like any other schema object.

**Table properties:**

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `rls` | boolean | `false` | Enable row-level security |
| `force_rls` | boolean | `false` | Force RLS even for table owner |
| `policies` | object[] | — | RLS policy definitions (see below) |

**Policy properties:**

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `name` | string | required | Policy name |
| `for` | string | required | `SELECT`, `INSERT`, `UPDATE`, `DELETE`, or `ALL` |
| `to` | string or string[] | `PUBLIC` | Role(s) the policy applies to |
| `using` | string | — | USING expression |
| `check` | string | — | WITH CHECK expression |
| `permissive` | boolean | `true` | `false` for RESTRICTIVE policy |

```yaml
table: orders
rls: true
force_rls: true

columns:
  - name: id
    type: serial
    primary_key: true
  - name: user_id
    type: integer

policies:
  - name: users_see_own_orders
    for: SELECT
    to: app_user
    using: "user_id = get_current_user_id()"

  - name: deny_suspended
    for: ALL
    permissive: false
    using: "NOT is_suspended()"
```

**Mixin example (tenant isolation):**

```yaml
# schema-flow/mixins/tenant_isolation.yaml
mixin: tenant_isolation
rls: true

columns:
  - name: tenant_id
    type: uuid

policies:
  - name: "{table}_tenant_isolation"
    for: ALL
    using: "tenant_id = current_setting('app.tenant_id')::uuid"
    check: "tenant_id = current_setting('app.tenant_id')::uuid"
```

When a mixin sets `rls: true`, all tables using it inherit RLS. The table can override with `rls: false`. Policy names support `{table}` interpolation, just like triggers and indexes.

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
- **Indexes, checks, triggers, policies**: mixin entries are added before table entries.
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
| `security` | string | — | `definer` or `invoker` (default: invoker). Use `definer` for functions that need elevated privileges (e.g., RLS helper functions). |

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

**Function with SECURITY DEFINER** (useful for RLS helper functions):

```yaml
# schema-flow/schema/fn_get_current_user_id.yaml
name: get_current_user_id
language: sql
returns: integer
security: definer
body: |
  SELECT id FROM users WHERE auth_id = current_setting('app.auth_id');
```

Function files (prefixed with `fn_`) are automatically detected and applied with `CREATE OR REPLACE FUNCTION` before schema migration runs. This ensures trigger functions exist before any triggers reference them.

## Drift Detection

Compare your YAML schema files against a live database and report bi-directional differences:

```bash
npx @mabulu-inc/schema-flow drift          # text report
npx @mabulu-inc/schema-flow drift --json   # JSON output
npx @mabulu-inc/schema-flow drift --quiet  # exit code only (0 = no drift, 1 = drift)
```

Drift detection finds:
- Tables in the DB with no matching YAML file
- Tables in YAML missing from the DB
- Column differences (type, nullable, default, unique)
- Missing or extra columns, triggers, policies, and functions
- RLS setting mismatches
- Enum value mismatches (missing or extra values)
- Missing or extra extensions
- View query differences
- Index mismatches (missing, extra, or different definition)
- Comment differences (table and column level)

## Migration Linting

Run static analysis on the migration plan to catch dangerous patterns before applying:

```bash
npx @mabulu-inc/schema-flow lint          # text report
npx @mabulu-inc/schema-flow lint --json   # JSON output
```

Built-in rules:

| Rule | Severity | Detects |
| --- | --- | --- |
| `lock/set-not-null-direct` | warning | SET NOT NULL without safe 4-step pattern |
| `lock/add-column-with-default` | info | ADD COLUMN with DEFAULT (safe PG11+, informational) |
| `data/drop-column` | warning | Any DROP COLUMN |
| `data/drop-table` | warning | Any DROP TABLE |
| `data/type-narrowing` | warning | Destructive type changes |
| `perf/missing-fk-index` | warning | FK columns without matching index |
| `compat/rename-detection` | info | Drop+add same-type columns (possible rename) |
| `compat/type-change` | warning | Any column type change |

Exit code is `1` if any error-severity findings are present.

## Down Migrations (Rollback)

Schema-flow records each migration run (operations + pre-migration snapshot). You can view and execute reverse migrations:

```bash
npx @mabulu-inc/schema-flow down                          # show reverse plan
npx @mabulu-inc/schema-flow down --apply                  # execute rollback
npx @mabulu-inc/schema-flow down --apply --allow-destructive  # include destructive reverses
```

Reverse operations are computed automatically from the forward plan:

| Forward | Reverse | Notes |
| --- | --- | --- |
| CREATE TABLE | DROP TABLE CASCADE | Destructive |
| ADD COLUMN | DROP COLUMN | Destructive |
| ALTER TYPE | ALTER TYPE back | Uses snapshot |
| SET NOT NULL | DROP NOT NULL | Safe |
| ADD INDEX | DROP INDEX CONCURRENTLY | Safe |
| ADD CONSTRAINT | DROP CONSTRAINT | Safe |
| CREATE TRIGGER | DROP TRIGGER | Safe |
| ENABLE RLS | DISABLE RLS | Safe |
| CREATE POLICY | DROP POLICY | Safe |
| DROP COLUMN/TABLE | Irreversible | Data gone |

## SQL File Generation

Generate a versioned SQL migration file from the current plan:

```bash
npx @mabulu-inc/schema-flow sql                              # default: migrations/ dir
npx @mabulu-inc/schema-flow sql --output-dir ./my-migrations  # custom directory
npx @mabulu-inc/schema-flow sql --name add_users              # custom name suffix
npx @mabulu-inc/schema-flow sql --output ./migrate.sql        # specific file
```

The generated file groups operations into proper transaction boundaries:

```
Functions         → BEGIN/COMMIT
Structure ops     → BEGIN/COMMIT
Indexes           → outside transaction (CONCURRENTLY)
Foreign keys      → BEGIN/COMMIT (NOT VALID)
Validate          → outside transaction
```

## Pre-Migration Checks

Add SQL assertions to your YAML schema that must pass before a migration runs:

```yaml
table: users
prechecks:
  - name: no_null_emails
    query: "SELECT count(*) = 0 FROM users WHERE email IS NULL"
    message: "All users must have emails before adding NOT NULL"
columns:
  - name: email
    type: varchar(255)
```

Each check query must return a single truthy value. If any check fails, the migration is aborted. Skip checks with `--skip-checks`.

## ERD / Mermaid Output

Generate a Mermaid ER diagram from your YAML schemas (no database connection required):

```bash
npx @mabulu-inc/schema-flow erd                    # stdout
npx @mabulu-inc/schema-flow erd --output schema.md  # write to file
```

Output example:

```
erDiagram
    USERS {
        serial id PK
        varchar(255) email UK
        text name
    }
    ORDERS {
        serial id PK
        integer user_id FK
    }
    USERS ||--o{ ORDERS : "user_id"
```

## Expand/Contract Pattern

For zero-downtime column renames, type changes, and transforms, schema-flow supports the expand/contract pattern (similar to pgroll). This avoids downtime by using dual-write triggers and batched backfills.

### 1. Define the expand

```yaml
table: users
columns:
  - name: full_name
    type: text
    nullable: true
    expand:
      from: name           # existing column to transform from
      transform: "name"    # SQL expression for old → new
      reverse: "full_name" # SQL expression for new → old (optional, enables reverse writes)
      batch_size: 5000     # rows per backfill batch (default: 1000)
```

### 2. Apply the migration

```bash
npx @mabulu-inc/schema-flow run
```

This performs the expand phase:
1. `ALTER TABLE ADD COLUMN full_name text` (nullable)
2. Creates a dual-write trigger function that syncs writes between old and new columns
3. Creates the trigger on the table
4. Runs a batched backfill (`FOR UPDATE SKIP LOCKED` to avoid blocking)

### 3. Check status

```bash
npx @mabulu-inc/schema-flow expand-status
```

### 4. Contract (finalize)

Once you've verified the backfill is complete and updated application code:

```bash
npx @mabulu-inc/schema-flow contract --allow-destructive
```

This:
1. Verifies no NULLs remain in the new column
2. Drops the dual-write trigger and function
3. Drops the old column

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
    schema/         ← One YAML file per table, function, enum, extension, or view
      users.yaml
      posts.yaml
      fn_update_timestamp.yaml
      enum_status.yaml
      extensions.yaml
      view_active_users.yaml
      mv_daily_stats.yaml
    mixins/         ← Reusable schema mixins
      timestamps.yaml
      soft_delete.yaml
    pre/            ← Pre-migration SQL scripts
      20260228153000_rename_column.sql
    post/           ← Post-migration SQL scripts
      20260228160000_seed_roles.sql
    repeatable/     ← SQL scripts re-run whenever content changes
      grants.sql
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
| `SCHEMA_FLOW_LOCK_TIMEOUT` | Lock timeout for DDL statements (default: `5s`) |
| `SCHEMA_FLOW_STATEMENT_TIMEOUT` | Statement timeout for DDL statements (default: `30s`) |

## Enums

Define PostgreSQL enum types in `enum_*.yaml` files:

```yaml
# schema-flow/schema/enum_status.yaml
enum: status
values:
  - active
  - inactive
  - suspended
```

Schema-flow manages the lifecycle automatically:
- **New enum** → `CREATE TYPE "public"."status" AS ENUM ('active', 'inactive', 'suspended')`
- **New value added** → `ALTER TYPE "public"."status" ADD VALUE 'suspended'`
- Values can be added but never removed (PostgreSQL limitation)

Use the enum type in a table column:

```yaml
table: users
columns:
  - name: status
    type: status
    default: "'active'"
```

## Extensions

Declare required PostgreSQL extensions in an `extensions.yaml` file:

```yaml
# schema-flow/schema/extensions.yaml
extensions:
  - pgcrypto
  - pg_trgm
  - uuid-ossp
```

Extensions are created with `CREATE EXTENSION IF NOT EXISTS` and run before all other operations. Dropping an extension requires `--allow-destructive`.

## Views & Materialized Views

**Views** use `view_*.yaml` files:

```yaml
# schema-flow/schema/view_active_users.yaml
view: active_users
query: "SELECT id, email FROM users WHERE is_active = true"
```

Views are created with `CREATE OR REPLACE VIEW`, so updates are safe and non-destructive.

**Materialized views** use `mv_*.yaml` files:

```yaml
# schema-flow/schema/mv_daily_stats.yaml
materialized_view: daily_stats
query: "SELECT date_trunc('day', created_at) AS day, count(*) AS total FROM events GROUP BY 1"
indexes:
  - columns: [day]
    unique: true
```

Materialized views support indexes. Dropping a materialized view requires `--allow-destructive`.

## Generated Columns

Declare computed stored columns using the `generated` property:

```yaml
table: users
columns:
  - name: first_name
    type: text
  - name: last_name
    type: text
  - name: full_name
    type: text
    generated: "first_name || ' ' || last_name"
```

This produces `GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED`. Generated columns cannot have `default` or `NOT NULL` — schema-flow handles this automatically.

**Limitation:** PostgreSQL does not support altering a generated column expression in place. To change an expression, drop and re-add the column (requires `--allow-destructive`).

## Table & Column Comments

Add descriptions to tables and columns:

```yaml
table: users
comment: "Core user accounts table"
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: varchar(255)
    comment: "User's primary email address"
```

This generates:
- `COMMENT ON TABLE "public"."users" IS 'Core user accounts table'`
- `COMMENT ON COLUMN "public"."users"."email" IS 'User''s primary email address'`

Comments are diffed against the live database — unchanged comments produce no operations.

## Deferrable Foreign Keys

Use `deferrable` and `initially_deferred` for self-referential tables, circular references, or tree structures where you need to insert parent and child rows in the same transaction:

```yaml
table: nodes
columns:
  - name: id
    type: serial
    primary_key: true
  - name: parent_id
    type: integer
    nullable: true
    references:
      table: nodes
      column: id
      deferrable: true
      initially_deferred: true
```

With `initially_deferred: true`, the FK constraint is only checked at transaction commit, allowing inserts in any order within the transaction.

## Environment Configuration

Create a `schema-flow.config.yaml` file in your project root to manage multiple environments:

```yaml
# schema-flow.config.yaml
environments:
  development:
    connectionString: postgresql://localhost:5432/myapp_dev
    pgSchema: public
  staging:
    connectionString: ${STAGING_DB_URL}
    pgSchema: public
  production:
    connectionString: ${PROD_DB_URL}
    pgSchema: app
```

Use `${VAR}` syntax for environment variable interpolation. Select an environment with `--env`:

```bash
npx @mabulu-inc/schema-flow run --env staging
npx @mabulu-inc/schema-flow drift --env production
```

**Precedence:** `--connection-string` flag > `--env` config > `SCHEMA_FLOW_DATABASE_URL` > `DATABASE_URL`

## Baseline

Mark an existing database as managed by schema-flow without running any migrations:

```bash
npx @mabulu-inc/schema-flow baseline
```

This records all current schema files in the `_schema_flow_history` table with their SHA-256 hashes, so subsequent runs only process new or changed files. Use this when adopting schema-flow on a database that already has the tables defined in your YAML.

## Repeatable Migrations

Place SQL files in `schema-flow/repeatable/` for scripts that should re-run whenever their content changes:

```
schema-flow/repeatable/
  grants.sql
  refresh_views.sql
```

Repeatable files are tracked by content hash. When the file content changes, the script is re-executed. When unchanged, it is skipped. Repeatables run after all other migration phases.

Common use cases: GRANT/REVOKE statements, materialized view refreshes, function re-definitions that live outside schema YAML.

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
