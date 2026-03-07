# schema-flow Roadmap

## Recently Shipped (v1.4.0-dev)

- **Role qualifiers:** `bypassrls` and `replication` in role YAMLs, with drift detection
- **Function qualifiers:** `volatility`, `parallel`, `strict`, `leakproof`, `cost`, `rows`, `set`
- **Schema grants:** `schema_grants` in extensions YAML (`GRANT USAGE ON SCHEMA`)
- **Seed `!sql` expressions:** `!sql` YAML tag for raw SQL in seed data (e.g., `!sql "now() - interval '1 day'"`)
- **Seed type casting:** VALUES clause casts to column types (fixes UUID/typed column matching)
- **Composite unique key seeds:** seed key resolution falls back to multi-column `unique_constraints`
- **Auto schema creation:** `CREATE SCHEMA IF NOT EXISTS` when `pgSchema` is not `public`
- **`description:` alias:** `description:` accepted as alias for `comment:` on tables, columns, functions, and roles
- **Seed `on_conflict: DO NOTHING`:** object-format seeds with `on_conflict:` and `rows:` keys; `DO NOTHING` skips the UPDATE phase (insert-only mode)
- **Column-level `check:` sugar:** inline `check:` on column definitions, auto-extracted into the table's `checks` array

## Up Next

### Seed Enhancements
- [ ] **Seed deletion** — detect rows in the table that are NOT in the seed list and optionally remove them (`seeds_prune: true`)
- [ ] **Conditional seeds** — `when:` guard on seed blocks (e.g., only seed in non-production environments)
- [ ] **Seed ordering** — explicit `depends_on` between tables to control seed execution order when FKs exist

### Drift Detection
- [ ] **Full drift detection mode** — `schema-flow drift` command that compares live DB state against all YAML files without requiring file changes
- [ ] **Drift report** — structured JSON output of all differences (roles, tables, columns, indexes, grants) for CI integration

### Function Management
- [ ] **Function diff/replace** — detect when function body or qualifiers have changed and issue `CREATE OR REPLACE` automatically
- [ ] **Function introspection** — `schema-flow generate` should capture `volatility`, `parallel`, `strict`, `leakproof`, `cost`, `set` from existing functions
- [ ] **Function drop** — remove functions no longer in YAML (behind `--allow-destructive`)

### Grant & Security
- [ ] **Default privileges** — `ALTER DEFAULT PRIVILEGES` support for automatic grants on new objects
- [ ] **Grant diff** — detect and revoke stale grants that no longer appear in YAML
- [ ] **Schema-level grants in planner** — idempotent `GRANT USAGE ON SCHEMA` that checks `has_schema_privilege()` before granting

### Migration Safety
- [ ] **Expand/contract for indexes** — `CREATE INDEX CONCURRENTLY` with automatic retry on failure
- [ ] **Lock timeout** — configurable `SET lock_timeout` before DDL to avoid long waits
- [ ] **Statement timeout** — configurable `SET statement_timeout` for safety
- [ ] **Migration history** — `schema-flow history` command showing past runs, timestamps, and operation counts

### Developer Experience
- [ ] **YAML validation** — `schema-flow validate` command that checks YAML syntax and type correctness without connecting to Postgres
- [ ] **Interactive plan** — `schema-flow plan --interactive` to approve/reject individual operations
- [ ] **Diff output** — side-by-side diff of current vs desired state in `plan` output
- [ ] **Watch mode** — `schema-flow watch` for local development (re-run on YAML changes)

### Schema Types
- [ ] **Domain types** — `CREATE DOMAIN` support for reusable constrained types
- [ ] **Composite types** — `CREATE TYPE ... AS (...)` for structured types
- [ ] **Sequences** — standalone sequence YAML definitions with ownership tracking
- [ ] **Partitioned tables** — `PARTITION BY` support in table YAMLs with sub-partition definitions

### Multi-Database
- [ ] **Multiple schemas in one run** — process multiple `pgSchema` targets in a single invocation
- [ ] **Cross-database references** — foreign data wrappers / dblink support in YAML

## Known Issues
- Multi-tenant-orders example test has a pre-existing RLS tenant isolation failure
- File tracker is change-driven, not drift-driven — unchanged YAML files skip planning even if the DB has drifted
