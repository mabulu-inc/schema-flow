# YAML Schema Reference

This is the definitive specification for every YAML key supported by schema-flow. All schema definitions live under the `schema/` directory in your project root.

## Directory Structure

```
schema/
  tables/          One YAML file per table
  enums/           One YAML file per enum type
  functions/       One YAML file per function
  views/           One YAML file per view (materialized views use mv_ prefix)
  roles/           One YAML file per database role
  mixins/          Reusable column/index/trigger/policy sets
  pre/             SQL scripts run BEFORE declarative migration
  post/            SQL scripts run AFTER declarative migration
  repeatable/      SQL scripts re-applied whenever their content changes
  extensions.yaml  PostgreSQL extensions to enable
```

Files may use `.yaml` or `.yml` extensions. Within each directory, files are sorted alphabetically.

---

## Table Schema

**Location:** `schema/tables/<table_name>.yaml`

The table name is derived from the filename if the `table` key is omitted.

### Complete Key Reference

```yaml
table: users
comment: "User accounts"
use: [timestamps, soft_delete]

columns:
  - name: id
    type: serial
    primary_key: true
    comment: "Primary key"

  - name: email
    type: varchar(255)
    nullable: false
    unique: true
    unique_name: uq_email
    default: "'unknown'"
    comment: "User email address"

  - name: org_id
    type: uuid
    references:
      table: organizations
      column: id
      schema: other_schema
      name: fk_users_org
      on_delete: CASCADE
      on_update: CASCADE
      deferrable: true
      initially_deferred: true

  - name: full_name
    type: text
    generated: "first_name || ' ' || last_name"

  - name: new_email
    type: varchar(255)
    expand:
      from: email
      transform: "lower(email)"
      reverse: "email"
      batch_size: 5000

primary_key: [tenant_id, id]
primary_key_name: pk_users

indexes:
  - columns: [email]
    unique: true
    name: idx_users_email
    method: btree
    where: "deleted_at IS NULL"
    include: [name]
    opclass: jsonb_path_ops
    comment: "Email lookup index"

checks:
  - name: chk_status
    expression: "status IN ('active', 'inactive')"
    comment: "Valid status values"

unique_constraints:
  - name: uq_tenant_email
    columns: [tenant_id, email]
    comment: "Unique email per tenant"

triggers:
  - name: trg_updated_at
    timing: BEFORE
    events: [UPDATE]
    function: set_updated_at
    for_each: ROW
    when: "OLD.* IS DISTINCT FROM NEW.*"
    comment: "Auto-update timestamp"

rls: true
force_rls: true

policies:
  - name: tenant_isolation
    for: ALL
    to: [app_user]
    using: "tenant_id = current_setting('app.tenant_id')::int"
    check: "tenant_id = current_setting('app.tenant_id')::int"
    permissive: false
    comment: "Tenant isolation policy"

grants:
  - to: [app_reader]
    privileges: [SELECT]
  - to: app_writer
    privileges: [SELECT, INSERT, UPDATE, DELETE]
    columns: [name, email]
    with_grant_option: true

prechecks:
  - name: data_valid
    query: "SELECT COUNT(*) = 0 FROM users WHERE email IS NULL"
    message: "Found users with null emails"

seeds:
  - id: 1
    email: admin@example.com
    status: active
  - id: 2
    email: user@example.com
    status: active
```

### Key-by-Key Specification

#### Top-Level Keys

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `table` | string | No | Derived from filename | Table name. |
| `comment` | string | No | - | PostgreSQL `COMMENT ON TABLE`. |
| `use` | string[] | No | - | List of mixin names to apply. |
| `columns` | ColumnDef[] | **Yes** | - | Column definitions (see below). |
| `primary_key` | string[] | No | - | Composite primary key columns. Alternative to column-level `primary_key: true`. |
| `primary_key_name` | string | No | Auto-generated | Custom name for the PK constraint. |
| `indexes` | IndexDef[] | No | - | Index definitions (see below). |
| `checks` | CheckDef[] | No | - | Check constraint definitions. |
| `unique_constraints` | UniqueConstraintDef[] | No | - | Multi-column unique constraints. |
| `triggers` | TriggerDef[] | No | - | Trigger definitions. |
| `rls` | boolean | No | - | Enable row-level security on this table. |
| `force_rls` | boolean | No | - | Force RLS even for the table owner. |
| `policies` | PolicyDef[] | No | - | Row-level security policy definitions. |
| `grants` | GrantDef[] | No | - | Table privilege grants. |
| `prechecks` | PrecheckDef[] | No | - | SQL assertions that must pass before migration runs. |
| `seeds` | object[] | No | - | Rows to upsert on every migration (keyed by primary key). |

#### Column Definition (`ColumnDef`)

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `name` | string | **Yes** | - | Column name. |
| `type` | string | **Yes** | - | Any PostgreSQL type: `serial`, `bigserial`, `integer`, `bigint`, `text`, `varchar(N)`, `uuid`, `boolean`, `timestamp`, `timestamptz`, `jsonb`, `text[]`, `numeric(P,S)`, enum types, etc. |
| `nullable` | boolean | No | `false` | Whether the column allows NULL. Columns are NOT NULL by default. |
| `default` | string | No | - | Default value as a SQL expression. String literals must be quoted: `"'hello'"`. Expressions like `now()` or `gen_random_uuid()` are used directly. |
| `primary_key` | boolean | No | `false` | Mark this column as the single-column primary key. Use the top-level `primary_key` array for composite keys instead. |
| `unique` | boolean | No | `false` | Create a single-column unique constraint. |
| `unique_name` | string | No | Auto-generated | Custom name for the unique constraint. |
| `references` | object | No | - | Foreign key reference (see below). |
| `generated` | string | No | - | SQL expression for a `GENERATED ALWAYS AS (expr) STORED` column. |
| `expand` | object | No | - | Expand/contract migration pattern (see below). |
| `comment` | string | No | - | PostgreSQL `COMMENT ON COLUMN`. |

#### Foreign Key Reference (`references`)

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `table` | string | **Yes** | - | Referenced table name. |
| `column` | string | **Yes** | - | Referenced column name. |
| `schema` | string | No | Same schema | Schema of the referenced table, for cross-schema foreign keys. |
| `name` | string | No | Auto-generated | Custom FK constraint name. |
| `on_delete` | string | No | `NO ACTION` | Referential action on delete. One of: `CASCADE`, `SET NULL`, `SET DEFAULT`, `RESTRICT`, `NO ACTION`. |
| `on_update` | string | No | `NO ACTION` | Referential action on update. Same options as `on_delete`. |
| `deferrable` | boolean | No | `false` | Whether the FK constraint is deferrable. |
| `initially_deferred` | boolean | No | `false` | Whether the FK constraint is initially deferred. Requires `deferrable: true`. |

#### Expand/Contract Pattern (`expand`)

Used for zero-downtime column renames and transforms. Creates a new column with a dual-write trigger and backfills existing data.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `from` | string | **Yes** | - | Source column name. |
| `transform` | string | **Yes** | - | SQL expression to transform source to target (e.g., `"lower(email)"`). |
| `reverse` | string | No | - | SQL expression for reverse transform (target to source), enabling dual-write. |
| `batch_size` | number | No | `1000` | Number of rows per backfill batch. |

#### Index Definition (`IndexDef`)

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `columns` | string[] | **Yes** | - | Columns to index. |
| `name` | string | No | Auto-generated | Index name. |
| `unique` | boolean | No | `false` | Whether the index enforces uniqueness. |
| `method` | string | No | `btree` | Index method. One of: `btree`, `gin`, `gist`, `hash`, `brin`. |
| `where` | string | No | - | Partial index condition (SQL expression). |
| `include` | string[] | No | - | Covering index columns (INCLUDE clause). |
| `opclass` | string | No | - | Operator class (e.g., `jsonb_path_ops`, `gin_trgm_ops`). |
| `comment` | string | No | - | PostgreSQL `COMMENT ON INDEX`. |

#### Check Constraint (`CheckDef`)

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `name` | string | No | Auto-generated | Constraint name. |
| `expression` | string | **Yes** | - | SQL boolean expression. |
| `comment` | string | No | - | Constraint comment. |

#### Unique Constraint (`UniqueConstraintDef`)

For multi-column unique constraints. Single-column unique constraints should use the column-level `unique: true` key instead.

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `name` | string | No | Auto-generated | Constraint name. |
| `columns` | string[] | **Yes** | - | Columns in the unique constraint. |
| `comment` | string | No | - | Constraint comment. |

#### Trigger Definition (`TriggerDef`)

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `name` | string | **Yes** | - | Trigger name. |
| `timing` | string | **Yes** | - | When the trigger fires. One of: `BEFORE`, `AFTER`, `INSTEAD OF`. |
| `events` | string[] | **Yes** | - | Events that fire the trigger. Array of: `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`. |
| `function` | string | **Yes** | - | Name of the function to execute. Must exist in `schema/functions/`. |
| `for_each` | string | No | `ROW` | Trigger granularity. One of: `ROW`, `STATEMENT`. |
| `when` | string | No | - | Optional SQL WHEN condition. |
| `comment` | string | No | - | Trigger comment. |

#### RLS Policy Definition (`PolicyDef`)

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `name` | string | **Yes** | - | Policy name. |
| `for` | string | **Yes** | - | Command the policy applies to. One of: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `ALL`. |
| `to` | string[] | No | `PUBLIC` | Roles the policy applies to. Omit for all roles. |
| `using` | string | No | - | USING expression (controls which rows are visible). |
| `check` | string | No | - | WITH CHECK expression (controls which rows can be written). |
| `permissive` | boolean | No | `true` | `true` for PERMISSIVE policy, `false` for RESTRICTIVE policy. |
| `comment` | string | No | - | Policy comment. |

#### Grant Definition (`GrantDef`)

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `to` | string or string[] | **Yes** | - | Role name or array of role names. |
| `privileges` | string[] | **Yes** | - | Privileges to grant. Array of: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, `REFERENCES`, `TRIGGER`, `ALL`. |
| `columns` | string[] | No | - | Restrict grant to specific columns (column-level grant). |
| `with_grant_option` | boolean | No | `false` | Whether the grantee can grant the same privileges to others. |

#### Precheck Definition (`PrecheckDef`)

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `name` | string | **Yes** | - | Check name (for error reporting). |
| `query` | string | **Yes** | - | SQL query that must return a truthy value for migration to proceed. |
| `message` | string | No | - | Custom failure message shown when the check fails. |

#### Seeds

Seeds are an array of objects. Each object is a row to upsert on every migration. The upsert is keyed on the table's primary key columns.

```yaml
seeds:
  - id: 1
    name: "Default Category"
    slug: default
  - id: 2
    name: "Premium Category"
    slug: premium
```

---

## Enum Schema

**Location:** `schema/enums/<name>.yaml`

Defines a PostgreSQL enum type. Schema-flow manages adding new values to existing enums. Enum values cannot be removed or reordered (a PostgreSQL limitation).

```yaml
enum: status_type
values: [active, inactive, pending]
comment: "User status values"
```

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `enum` | string | **Yes** | - | Enum type name. |
| `values` | string[] | **Yes** | - | Ordered list of enum values. Must be non-empty. |
| `comment` | string | No | - | PostgreSQL `COMMENT ON TYPE`. |

---

## Function Schema

**Location:** `schema/functions/<name>.yaml`

Defines a PostgreSQL function or trigger function.

```yaml
name: set_updated_at
language: plpgsql
returns: trigger
security: definer
replace: true
body: |
  BEGIN
    NEW.updated_at = now();
    RETURN NEW;
  END;
grants:
  - to: [app_user]
    privileges: [EXECUTE]
comment: "Auto-set updated_at timestamp"
```

Function with arguments and defaults:

```yaml
name: begin_session
language: plpgsql
returns: void
security: definer
args:
  - name: p_role
    type: text
  - name: p_user_id
    type: text
  - name: p_tenant_id
    type: text
    default: "''"
```

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `name` | string | **Yes** | - | Function name. The key `function` is also accepted as an alias for `name`. |
| `language` | string | No | `plpgsql` | Function language (`plpgsql`, `sql`, `plpython3u`, etc.). |
| `returns` | string | No | `void` | Return type (`trigger`, `void`, `integer`, `text`, `setof record`, etc.). |
| `args` | FunctionArg[] | No | `[]` | Function arguments as an array of `{name, type, mode?, default?}` objects (see below). |
| `body` | string | **Yes** | - | Function body. Use YAML block scalar (`\|`) for multi-line. |
| `replace` | boolean | No | `true` | Use `CREATE OR REPLACE`. Set to `false` to use `CREATE`. |
| `security` | string | No | `invoker` | Security mode. `definer` runs as the function owner; `invoker` (default) runs as the calling user. |
| `grants` | FunctionGrantDef[] | No | - | Function privilege grants (see below). |
| `comment` | string | No | - | PostgreSQL `COMMENT ON FUNCTION`. |

#### Function Argument (`FunctionArg`)

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `name` | string | **Yes** | - | Parameter name (e.g., `p_user_id`). |
| `type` | string | **Yes** | - | PostgreSQL type (e.g., `integer`, `text`, `uuid`, `text[]`). |
| `mode` | string | No | `in` | Parameter mode: `in`, `out`, `inout`, or `variadic`. |
| `default` | string | No | - | Default value expression (e.g., `"NULL"`, `"'hello'"`). |

#### Function Grant (`FunctionGrantDef`)

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `to` | string or string[] | **Yes** | - | Role name or array of role names. |
| `privileges` | string[] | **Yes** | - | Must be `[EXECUTE]`. Only `EXECUTE` is valid for functions. |

---

## View Schema

**Location:** `schema/views/<name>.yaml`

Defines a PostgreSQL view. Do not use the `mv_` filename prefix for regular views.

```yaml
view: active_users
query: |
  SELECT id, email
  FROM users
  WHERE status = 'active'
grants:
  - to: [app_reader]
    privileges: [SELECT]
comment: "Active users only"
```

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `view` | string | **Yes** | - | View name. |
| `query` | string | **Yes** | - | SQL query defining the view. |
| `grants` | GrantDef[] | No | - | View privilege grants (same structure as table grants). |
| `comment` | string | No | - | PostgreSQL `COMMENT ON VIEW`. |

---

## Materialized View Schema

**Location:** `schema/views/mv_<name>.yaml`

Defines a PostgreSQL materialized view. The filename **must** start with `mv_` to distinguish it from regular views. Both file types live in the same `views/` directory.

```yaml
materialized_view: mv_user_stats
query: |
  SELECT status, count(*) as cnt
  FROM users
  GROUP BY status
indexes:
  - columns: [status]
    unique: true
    name: idx_mv_user_stats_status
grants:
  - to: [app_reader]
    privileges: [SELECT]
comment: "User statistics cache"
```

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `materialized_view` | string | **Yes** | - | Materialized view name. |
| `query` | string | **Yes** | - | SQL query defining the materialized view. |
| `indexes` | IndexDef[] | No | - | Indexes on the materialized view (same structure as table indexes). |
| `grants` | GrantDef[] | No | - | Materialized view privilege grants. |
| `comment` | string | No | - | PostgreSQL `COMMENT ON MATERIALIZED VIEW`. |

---

## Role Schema

**Location:** `schema/roles/<name>.yaml`

Defines a PostgreSQL role. Schema-flow creates roles and manages role membership.

```yaml
role: app_reader
login: false
inherit: true
in: [app_base]
connection_limit: -1
comment: "Read-only application role"
```

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `role` | string | **Yes** | - | Role name. |
| `login` | boolean | No | `false` | Whether the role can log in. |
| `superuser` | boolean | No | `false` | Superuser privilege. |
| `createdb` | boolean | No | `false` | Can create databases. |
| `createrole` | boolean | No | `false` | Can create other roles. |
| `inherit` | boolean | No | `true` | Inherits privileges of granted roles. |
| `connection_limit` | number | No | `-1` | Maximum concurrent connections. `-1` means unlimited. |
| `in` | string[] | No | - | Roles this role is a member of (`GRANT <role> TO <this_role>`). |
| `comment` | string | No | - | PostgreSQL `COMMENT ON ROLE`. |

---

## Extensions Schema

**Location:** `schema/extensions.yaml`

Lists PostgreSQL extensions to enable via `CREATE EXTENSION IF NOT EXISTS`.

```yaml
extensions:
  - uuid-ossp
  - pgcrypto
  - pg_trgm
```

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `extensions` | string[] | **Yes** | - | List of extension names to enable. |

---

## Mixin Schema

**Location:** `schema/mixins/<name>.yaml`

Mixins are reusable sets of columns, indexes, triggers, checks, RLS policies, and grants. Tables apply mixins with the `use` key. Mixin columns are prepended to the table's column list; if a table defines a column with the same name as a mixin column, the table's definition wins.

Use the `{table}` placeholder in `name`, `expression`, `where`, `using`, and `check` fields -- it is replaced with the consuming table's name at expansion time.

```yaml
mixin: timestamps

columns:
  - name: created_at
    type: timestamptz
    default: now()
  - name: updated_at
    type: timestamptz
    default: now()

indexes:
  - name: idx_{table}_created_at
    columns: [created_at]

triggers:
  - name: set_{table}_updated_at
    timing: BEFORE
    events: [UPDATE]
    function: set_updated_at
    for_each: ROW
    when: "OLD.* IS DISTINCT FROM NEW.*"
```

| Key | Type | Required | Default | Description |
|-----|------|----------|---------|-------------|
| `mixin` | string | No | Derived from filename | Mixin name. |
| `columns` | ColumnDef[] | No | - | Columns to add (same structure as table columns). |
| `indexes` | IndexDef[] | No | - | Indexes to add. |
| `checks` | CheckDef[] | No | - | Check constraints to add. |
| `triggers` | TriggerDef[] | No | - | Triggers to add. |
| `rls` | boolean | No | - | Enable row-level security on consuming tables. |
| `force_rls` | boolean | No | - | Force RLS on consuming tables. |
| `policies` | PolicyDef[] | No | - | RLS policies to add. |
| `grants` | GrantDef[] | No | - | Grants to add. |

---

## Pre/Post Migration Scripts

**Location:** `schema/pre/*.sql` and `schema/post/*.sql`

Plain SQL scripts executed in alphabetical filename order. Pre-migration scripts run before declarative schema changes; post-migration scripts run after.

Use the CLI to scaffold timestamped scripts:

```
schema-flow new pre <name>     # Creates schema/pre/YYYYMMDDHHmmss_<name>.sql
schema-flow new post <name>    # Creates schema/post/YYYYMMDDHHmmss_<name>.sql
```

Each script is tracked in the history table and runs only once.

---

## Repeatable Scripts

**Location:** `schema/repeatable/*.sql`

SQL scripts that are re-applied whenever their content changes (tracked by content hash). Use these for idempotent operations like refreshing materialized views, reapplying grants, or rebuilding computed data.

---

## Configuration File

**Location:** `schema-flow.config.yaml` (project root or `schema/` directory)

Optional configuration file for environment-specific settings. Values support `${VAR}` interpolation from environment variables.

```yaml
environments:
  development:
    connectionString: postgresql://localhost:5432/myapp_dev
    pgSchema: public
    lockTimeout: "5s"
    statementTimeout: "30s"

  staging:
    connectionString: ${STAGING_DATABASE_URL}
    pgSchema: public

  production:
    connectionString: ${DATABASE_URL}
    pgSchema: public
    lockTimeout: "10s"
    statementTimeout: "60s"
```

### Environment Config Keys

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `connectionString` | string | `$SCHEMA_FLOW_DATABASE_URL` or `$DATABASE_URL` | PostgreSQL connection string. |
| `pgSchema` | string | `public` | PostgreSQL schema name. |
| `lockTimeout` | string | `5s` | Lock timeout for DDL statements. Set to `"0"` to disable. |
| `statementTimeout` | string | `30s` | Statement timeout for DDL statements. Set to `"0"` to disable. |

The configuration file is searched in these locations (first match wins):

1. `<project>/schema-flow.config.yaml`
2. `<project>/schema-flow.config.yml`
3. `<project>/schema/schema-flow.config.yaml`
4. `<project>/schema/schema-flow.config.yml`

---

## Environment Variables

These environment variables configure schema-flow when a config file is not used:

| Variable | Description |
|----------|-------------|
| `SCHEMA_FLOW_DATABASE_URL` | PostgreSQL connection string (highest priority). |
| `DATABASE_URL` | PostgreSQL connection string (fallback). |
| `SCHEMA_FLOW_ALLOW_DESTRUCTIVE` | Set to `true` to allow destructive operations (column drops, table drops). |
| `SCHEMA_FLOW_LOCK_TIMEOUT` | Lock timeout for DDL (default: `5s`). |
| `SCHEMA_FLOW_STATEMENT_TIMEOUT` | Statement timeout for DDL (default: `30s`). |
| `SCHEMA_FLOW_MAX_RETRIES` | Max retries for transient DB errors (default: `3`). |
