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
- **Convention over configuration** — Drop files in `schema/tables/`, `schema/pre/`, `schema/post/`. That's it.
- **Surgical change detection** — filepath + SHA-256 hash tracking means only new or changed files are processed.
- **Safe FK ordering** — Tables are created first (without foreign keys), then all FKs are added after every table exists.
- **CI/CD ready** — Non-zero exit codes on failure, structured logging, `--quiet` mode for pipelines.
- **Plan before apply** — `schema-flow plan` shows exactly what SQL would run, without touching the database.
- **Phase control** — Run `pre`, `migrate`, `post` independently or all at once.
- **Generate from existing DB** — Bootstrap your schema files from a live database with `schema-flow generate`.

## Setup

This package is published to **GitHub Packages**. Add this to `.npmrc` in your project root:

```ini
@mabulu-inc:registry=https://npm.pkg.github.com
//npm.pkg.github.com/:_authToken=${GITHUB_TOKEN}
```

Set a [personal access token](https://github.com/settings/tokens/new?scopes=read:packages) with `read:packages` scope:

```bash
export GITHUB_TOKEN=ghp_your_token_here
```

> In GitHub Actions, `GITHUB_TOKEN` is available automatically.

## Quick Start

### 1. Initialize the directory structure

```bash
npx @mabulu-inc/schema-flow init
```

This creates:

```
schema/
  tables/     ← Declarative YAML table definitions (one file per table)
  enums/      ← Enum type definitions
  functions/  ← Function definitions
  views/      ← View and materialized view definitions
  roles/      ← Role definitions
  pre/        ← Pre-migration SQL scripts (run before schema changes)
  post/       ← Post-migration SQL scripts (run after schema changes)
  mixins/     ← Reusable schema mixins (timestamps, soft_delete, etc.)
  repeatable/ ← SQL scripts re-run whenever their content changes (refresh, etc.)
```

### 2. Define your tables

**schema/tables/users.yaml**

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

**schema/tables/posts.yaml**

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
| `schema-flow init` | Create `schema/` directory with `tables/`, `enums/`, `functions/`, `views/`, `pre/`, `post/` subdirectories |
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
| Set comment (any object) | ✓ | |
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

> **`plan` vs `validate`**: `plan` generates and displays the SQL but never sends it to PostgreSQL. `validate` executes the SQL inside a transaction that always rolls back — Postgres itself checks syntax, references, and types, catching errors that `plan` cannot.

## Migration Recipes

Most migrations are a single YAML edit. For everything else, schema-flow provides pre/post SQL scripts and the expand/contract pattern. This section shows how to handle every type of change.

### Edit your YAML

These changes require nothing more than editing your schema file and running `schema-flow run`. Schema-flow diffs the YAML against the live database and generates the correct SQL.

**Add a column**

```yaml
columns:
  - name: phone
    type: varchar(20)
    nullable: true
```

**Drop a column** — remove it from the YAML, run with `--allow-destructive`.

**Make a column NOT NULL** — set `nullable: false` (or remove `nullable` — NOT NULL is the default). Schema-flow uses a [safe 4-step pattern](#safe-not-null-pg-12) that avoids full-table locks.

**Make a column nullable** — set `nullable: true`. Instant, no table rewrite.

**Change a default** — edit the `default` value. Only affects future rows. To backfill existing rows, add a [post-migration script](#use-a-post-migration-script).

```yaml
  - name: status
    type: text
    default: "'active'"   # was "'pending'"
```

**Widen a column type** — change the type. Safe widening (e.g., `integer` → `bigint`) runs automatically. Narrowing requires `--allow-destructive`.

```yaml
  - name: counter
    type: bigint   # was integer
```

**Add an index** — append to `indexes`. Created with `CREATE INDEX CONCURRENTLY` — no table lock.

```yaml
indexes:
  - columns: [email]
    unique: true
  - columns: [status, created_at]
    where: "status = 'active'"
```

**Add a foreign key** — add `references` to the column. Created as `NOT VALID` then validated separately, avoiding heavy locks.

```yaml
  - name: author_id
    type: integer
    references:
      table: users
      column: id
      on_delete: CASCADE
```

**Add a check constraint**

```yaml
checks:
  - name: chk_positive_amount
    expression: "amount > 0"
```

**Add a unique constraint** — set `unique: true` on the column. Uses `CREATE UNIQUE INDEX CONCURRENTLY` under the hood.

**Add a trigger**

```yaml
triggers:
  - name: set_updated_at
    timing: BEFORE
    events: [UPDATE]
    function: update_timestamp
    for_each: ROW
```

**Enable row-level security**

```yaml
rls: true
policies:
  - name: users_own_data
    for: ALL
    using: "user_id = current_setting('app.user_id')::int"
```

**Create an enum** — add an `enum_*.yaml` file. Append new values to the list at any time; values can never be removed (PostgreSQL limitation).

```yaml
# schema/enums/status.yaml
enum: status
values: [active, inactive, suspended]
```

**Add an extension** — append to the extensions list.

```yaml
# schema/extensions.yaml
extensions:
  - pgcrypto
  - pg_trgm
```

**Create a view**

```yaml
# schema/views/active_users.yaml
view: active_users
query: "SELECT id, email FROM users WHERE is_active = true"
```

**Create a materialized view**

```yaml
# schema/views/mv_daily_stats.yaml
materialized_view: daily_stats
query: "SELECT date_trunc('day', created_at) AS day, count(*) FROM events GROUP BY 1"
indexes:
  - columns: [day]
    unique: true
```

**Add a generated column**

```yaml
  - name: full_name
    type: text
    generated: "first_name || ' ' || last_name"
```

**Add a comment** — works on tables, columns, indexes, triggers, checks, policies, enums, views, and functions.

```yaml
table: users
comment: "Core user accounts"
columns:
  - name: email
    type: varchar(255)
    comment: "Primary email address"
```

### Use a pre-migration script

For operations that YAML can't express — renames, data transforms, conditional DDL. Pre-migration scripts run **before** the declarative diff, so the database is already in the right state when schema-flow compares YAML to the live schema.

```bash
npx @mabulu-inc/schema-flow new pre <name>
```

**Rename a column** — rename in SQL, then update the YAML to use the new name.

```sql
BEGIN;
ALTER TABLE "public"."users" RENAME COLUMN "name" TO "display_name";
COMMIT;
```

**Rename a table** — rename in SQL, then rename the YAML file and update `table:`.

```sql
BEGIN;
ALTER TABLE "public"."users" RENAME TO "accounts";
COMMIT;
```

**Data migration**

```sql
BEGIN;
UPDATE "public"."orders" SET status = 'active' WHERE status IS NULL;
COMMIT;
```

> If you rename a column in YAML without a pre-migration script, schema-flow sees a DROP + ADD — it won't detect the rename automatically. The `compat/rename-detection` lint rule will warn you, but it won't fix it for you.

### Use expand/contract for zero-downtime transforms

When a column change would lock a large table, use expand/contract. Schema-flow adds the new column alongside the old one, installs a dual-write trigger, backfills in batches, and then drops the old column on your command. See [Expand/Contract Pattern](#expandcontract-pattern) for the full lifecycle.

**Zero-downtime column rename**

```yaml
  - name: full_name
    type: text
    nullable: true
    expand:
      from: name
      transform: "name"
      reverse: "full_name"
```

**Zero-downtime type change** — same column name, different type.

```yaml
  - name: counter
    type: bigint
    nullable: true
    expand:
      from: counter
      transform: "counter"
      reverse: "counter"
```

**Split a column**

```yaml
  - name: first_name
    type: text
    nullable: true
    expand:
      from: name
      transform: "split_part(name, ' ', 1)"

  - name: last_name
    type: text
    nullable: true
    expand:
      from: name
      transform: "split_part(name, ' ', 2)"
```

**Merge columns**

```yaml
  - name: full_name
    type: text
    nullable: true
    expand:
      from: first_name
      transform: "first_name || ' ' || last_name"
      reverse: "split_part(full_name, ' ', 1)"
```

After running the expand, verify and finalize:

```bash
npx @mabulu-inc/schema-flow run             # adds column + trigger + backfill
npx @mabulu-inc/schema-flow expand-status    # check progress
npx @mabulu-inc/schema-flow contract --allow-destructive  # drop old column
```

### Use a post-migration script

Post-migration scripts run **after** the declarative diff. Use them for seed data, grants, and cleanup.

```bash
npx @mabulu-inc/schema-flow new post <name>
```

**Seed data**

```sql
BEGIN;
INSERT INTO "public"."roles" (name) VALUES ('admin'), ('editor'), ('viewer')
ON CONFLICT (name) DO NOTHING;
COMMIT;
```

**Grants** — for grants that don't fit the declarative YAML model (e.g., `GRANT ... ON ALL TABLES IN SCHEMA`).

```sql
GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly_role;
```

For table-level and column-level grants, prefer the declarative `grants:` key in table YAML — see [Roles & Grants](#roles--row-level-security).

### Choosing the right approach

| Scenario | Approach |
| --- | --- |
| Add/modify columns, indexes, constraints, triggers, policies | Edit YAML |
| Rename a column (small table) | Pre-migration script |
| Rename a column (large table, zero downtime) | Expand/contract |
| Rename a table | Pre-migration script |
| Change column type (small table) | Edit YAML |
| Change column type (large table, zero downtime) | Expand/contract |
| Split or merge columns | Expand/contract |
| Backfill existing rows | Pre- or post-migration script |
| Seed data | Post-migration script |
| Table/column grants | Declarative `grants:` in table YAML |
| Schema-wide grants | Post-migration script |

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
| `comment` | string | — | Trigger description (`COMMENT ON TRIGGER`) |

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
| `comment` | string | — | Policy description (`COMMENT ON POLICY`) |

PostgreSQL combines policies as follows: all **PERMISSIVE** policies for a command are OR'd (any one passing is enough), and all **RESTRICTIVE** policies are AND'd on top (every one must pass). If a role has no matching policy for a command, that command is denied.

#### Step 1: Extract the tenant boundary into a mixin

A RESTRICTIVE policy that every table inherits. No permissive policy can bypass it.

```yaml
# schema/mixins/tenant_isolation.yaml
mixin: tenant_isolation
rls: true
force_rls: true

columns:
  - name: tenant_id
    type: uuid

policies:
  - name: "{table}_tenant_isolation"
    for: ALL
    permissive: false
    using: "tenant_id = current_setting('app.tenant_id')::uuid"
    check: "tenant_id = current_setting('app.tenant_id')::uuid"
```

`force_rls: true` ensures even the table owner (your migration role) is subject to RLS at runtime. Policy names support `{table}` interpolation — each table gets a uniquely-named constraint.

#### Step 2: Define per-role policies on the table

```yaml
# schema/tables/orders.yaml
table: orders
use: [tenant_isolation]

columns:
  - name: id
    type: serial
    primary_key: true
  - name: user_id
    type: integer
  - name: amount
    type: integer
  - name: status
    type: text
    default: "'draft'"
  - name: payment_method
    type: text
    nullable: true
  - name: created_at
    type: timestamptz
    default: now()

policies:
  # Users: read and create their own orders, edit only drafts/pending
  - name: users_select_own
    for: SELECT
    to: [app_user]
    using: "user_id = current_setting('app.user_id')::int"

  - name: users_insert_own
    for: INSERT
    to: [app_user]
    check: "user_id = current_setting('app.user_id')::int"

  - name: users_update_own
    for: UPDATE
    to: [app_user]
    using: "user_id = current_setting('app.user_id')::int"
    check: "status IN ('draft', 'pending')"

  # Managers: full read/write within the tenant
  - name: managers_select
    for: SELECT
    to: [app_manager]
    using: "true"

  - name: managers_update
    for: UPDATE
    to: [app_manager]
    using: "true"
    check: "true"

  # Auditors: read-only
  - name: auditors_select
    for: SELECT
    to: [app_auditor]
    using: "true"

  # Service role: unrestricted within tenant
  - name: service_all
    for: ALL
    to: [app_service]
    using: "true"
    check: "true"
```

The mixin contributes `tenant_id`, `rls: true`, `force_rls: true`, and the RESTRICTIVE tenant isolation policy. The table adds role-specific PERMISSIVE policies on top.

#### What each role can do

Every operation must also pass the `tenant_isolation` RESTRICTIVE policy — no role can ever reach another tenant's rows.

| Role | SELECT | INSERT | UPDATE | DELETE |
| --- | --- | --- | --- | --- |
| `app_user` | own orders | own orders | own draft/pending | denied |
| `app_manager` | all in tenant | denied | all in tenant | denied |
| `app_auditor` | all in tenant | denied | denied | denied |
| `app_service` | all in tenant | all in tenant | all in tenant | all in tenant |

A role with no matching PERMISSIVE policy for a command is denied — `app_auditor` has no INSERT/UPDATE/DELETE policies, so those commands return zero rows.

#### Step 3: Column-level access

RLS controls which **rows** a role can see. To control which **columns** a role can see, add column-level grants in the table YAML:

```yaml
# schema/tables/orders.yaml (grants section)
grants:
  # Users: full table access (RLS restricts rows)
  - to: app_user
    privileges: [SELECT, INSERT, UPDATE]

  # Managers: full table access
  - to: app_manager
    privileges: [SELECT, UPDATE]

  # Auditors: read-only, payment details hidden
  - to: app_auditor
    privileges: [SELECT]
    columns: [id, tenant_id, user_id, amount, status, created_at]

  # Service: full access
  - to: app_service
    privileges: [ALL]
```

Column-level grants compose with RLS — `app_auditor` can only read the listed columns, and only for rows passing both the tenant isolation and `auditors_select` policies.

For more complex masking (e.g., showing a redacted value instead of hiding the column entirely), use a view:

```yaml
# schema/views/orders_audit.yaml
view: orders_audit
query: |
  SELECT id, tenant_id, user_id, amount, status,
         '****' || right(payment_method, 4) AS payment_method,
         created_at
  FROM orders
comment: "Audit view with masked payment details"
```

Then grant auditors access to the view instead of (or in addition to) the base table.

### Mixins

Mixins let you DRY up repeated schema patterns (timestamps, soft delete, tenant scoping, etc.). Define a mixin once in `schema/mixins/`, then apply it to any table with `use:`.

**schema/mixins/timestamps.yaml**

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
| `comment` | string | — | Function description (`COMMENT ON FUNCTION`) |

```yaml
# schema/functions/update_timestamp.yaml
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
# schema/functions/get_current_user_id.yaml
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
- Comment differences (tables, columns, indexes, triggers, constraints, policies, functions)

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

For operations that can't be expressed declaratively — column renames, data migrations, schema-wide grants — use SQL scripts.

### Scaffold a script

```bash
npx @mabulu-inc/schema-flow new pre rename_user_email
# → schema/pre/20260228153000_rename_user_email.sql

npx @mabulu-inc/schema-flow new post seed_roles
# → schema/post/20260228153005_seed_roles.sql
```

Scripts are executed in alphabetical order. The UTC timestamp prefix ensures correct ordering.

### Example pre-migration script

```sql
-- schema/pre/20260228153000_rename_user_email.sql
BEGIN;
ALTER TABLE "public"."users" RENAME COLUMN "name" TO "display_name";
COMMIT;
```

### Example post-migration script

```sql
-- schema/post/20260228160000_seed_roles.sql
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

This introspects every table, function, enum, view, materialized view, role, and extension in the `public` schema and writes YAML files into `schema/tables/`, `schema/functions/`, `schema/enums/`, `schema/views/`, `schema/roles/`, etc. Table-level grants, view grants, materialized view grants, and function grants are all included in the generated YAML.

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

schema-flow looks for a `schema/` directory in the current working directory (or the directory specified with `--dir`):

```
my-project/
  schema/
    tables/         ← One YAML file per table
      users.yaml
      posts.yaml
    enums/          ← One YAML file per enum type
      status.yaml
    functions/      ← One YAML file per function
      update_timestamp.yaml
    views/          ← Views and materialized views
      active_users.yaml
      mv_daily_stats.yaml
    roles/          ← Role definitions
    mixins/         ← Reusable schema mixins
      timestamps.yaml
      soft_delete.yaml
    pre/            ← Pre-migration SQL scripts
      20260228153000_rename_column.sql
    post/           ← Post-migration SQL scripts
      20260228160000_seed_roles.sql
    repeatable/     ← SQL scripts re-run whenever content changes
      grants.sql
    extensions.yaml ← PostgreSQL extensions to enable
  src/
  package.json
```

Run all commands from your project root — schema-flow automatically finds the `schema/` folder.

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
# schema/enums/status.yaml
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
# schema/extensions.yaml
extensions:
  - pgcrypto
  - pg_trgm
  - uuid-ossp
```

Extensions are created with `CREATE EXTENSION IF NOT EXISTS` and run before all other operations. Dropping an extension requires `--allow-destructive`.

## Views & Materialized Views

**Views** use `view_*.yaml` files:

```yaml
# schema/views/active_users.yaml
view: active_users
query: "SELECT id, email FROM users WHERE is_active = true"
```

Views are created with `CREATE OR REPLACE VIEW`, so updates are safe and non-destructive.

**Materialized views** use `mv_*.yaml` files:

```yaml
# schema/views/mv_daily_stats.yaml
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

## Comments

Add descriptions to any schema object. Comments are diffed against the live database — unchanged comments produce no operations.

### Tables and Columns

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

### Indexes, Triggers, Checks, and Policies

```yaml
table: orders
rls: true
columns:
  - name: id
    type: serial
    primary_key: true
  - name: amount
    type: integer
indexes:
  - columns: [amount]
    name: idx_orders_amount
    comment: "Speed up amount range queries"
triggers:
  - name: trg_audit
    timing: AFTER
    events: [INSERT]
    function: audit_log
    comment: "Audit trail for new orders"
checks:
  - name: chk_positive_amount
    expression: "amount > 0"
    comment: "Amount must be positive"
policies:
  - name: orders_read
    for: SELECT
    using: "true"
    comment: "Allow all reads"
```

### Enums, Views, Materialized Views, and Functions

```yaml
# enum_status.yaml
enum: status
values: [active, inactive]
comment: "Account status type"

# view_active_users.yaml
view: active_users
query: "SELECT * FROM users WHERE is_active"
comment: "Only active users"

# mv_daily_stats.yaml
materialized_view: daily_stats
query: "SELECT count(*) FROM events"
comment: "Cached event statistics"

# fn_audit_log.yaml
name: audit_log
language: plpgsql
returns: trigger
body: "BEGIN RETURN NEW; END;"
comment: "Audit logging function"
```

All comments generate the appropriate `COMMENT ON <object>` SQL and are included in drift detection, scaffolding, and migration planning.

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

Place SQL files in `schema/repeatable/` for scripts that should re-run whenever their content changes:

```
schema/repeatable/
  refresh_views.sql
```

Repeatable files are tracked by content hash. When the file content changes, the script is re-executed. When unchanged, it is skipped. Repeatables run after all other migration phases.

Common use cases: materialized view refreshes, function re-definitions that live outside schema YAML, schema-wide GRANT/REVOKE statements that don't fit the declarative `grants:` model.

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

## Examples

The [`examples/`](examples/) directory contains three complete projects with schemas and integration tests:

| Example | Complexity | What it demonstrates |
| --- | --- | --- |
| [**todo-app**](examples/todo-app/) | Beginner | Tables, enums, FKs, indexes, check constraints, mixins, triggers |
| [**cms**](examples/cms/) | Intermediate | Roles, RLS, views, materialized views, seed data, soft delete, extensions |
| [**multi-tenant-orders**](examples/multi-tenant-orders/) | Advanced | Tenant isolation, modular roles, security functions, generated columns, column-level grants |

Each example includes an `example.test.ts` that migrates a fresh database and verifies behavior end-to-end:

```bash
npx vitest run examples/
```

See the [examples README](examples/README.md) for details.

## Switching to schema-flow

Already using another migration tool? We have guides:

- [**Switching from imperative tools**](docs/switching-from-imperative.md) — Knex, Flyway, Liquibase, golang-migrate, dbmate, raw SQL files
- [**Switching from ORM migrations**](docs/switching-from-orms.md) — Prisma, TypeORM, Sequelize, Django, Rails, SQLAlchemy/Alembic

TL;DR: `schema-flow generate` reads your existing DB and produces YAML. `schema-flow baseline` marks it as applied. Start writing YAML from there.

## Documentation

- [YAML Reference](docs/yaml-reference.md) — Complete specification of every YAML key
- [AI Integration](docs/CLAUDE.md.template) — CLAUDE.md template for your project
- [docs/](docs/) — All documentation

## License

MIT — see [LICENSE](LICENSE).
