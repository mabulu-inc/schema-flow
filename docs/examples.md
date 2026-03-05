# Examples

The [`examples/`](https://github.com/mabulu-inc/schema-flow/tree/main/examples) directory contains four complete projects that progress from simple to complex. Each includes an integration test that migrates a fresh database and verifies behavior.

## [Todo App](https://github.com/mabulu-inc/schema-flow/tree/main/examples/todo-app)

**Complexity:** Beginner

The simplest starting point — a single-user todo list.

**Features covered:**
- Tables with columns, types, and defaults
- Enums (`priority`)
- Foreign keys
- Indexes
- Check constraints
- Mixins (`timestamps`)
- Trigger functions

## [CMS](https://github.com/mabulu-inc/schema-flow/tree/main/examples/cms)

**Complexity:** Intermediate

A multi-role content management system.

**Features covered:** everything in todo-app, plus:
- PostgreSQL extensions (`pgcrypto`, `pg_trgm`)
- Roles (`cms_admin`, `cms_editor`, `cms_author`)
- Row-level security with PERMISSIVE policies
- Views and materialized views
- Seed data
- Soft delete pattern
- Comments on all object types
- GIN indexes with `gin_trgm_ops`
- `SECURITY DEFINER` functions
- Table and column grants

## [Multi-Tenant Orders](https://github.com/mabulu-inc/schema-flow/tree/main/examples/multi-tenant-orders)

**Complexity:** Advanced

A multi-tenant order management system with membership-based access control.

**Features covered:** everything in CMS, plus:
- Tenant isolation via RESTRICTIVE RLS policies
- Modular roles (multiple roles per user per tenant)
- Security functions (`register_account`, `create_membership`, `begin_session`, `grant_role`, `revoke_role`)
- Generated columns
- Column-level grants
- Audit trail
- Cross-tenant admin patterns

## [Multi-Schema](https://github.com/mabulu-inc/schema-flow/tree/main/examples/multi-schema)

**Complexity:** Advanced

An analytics platform split across two PostgreSQL schemas.

**Features covered:**
- Cross-schema foreign keys (`references.schema`)
- Cross-schema views
- Migration ordering (core first, analytics second)
- Separate schema directories

## Running the Tests

Every example includes an `example.test.ts` that creates a fresh database and verifies end-to-end behavior.

```bash
# Run all examples
npx vitest run examples/

# Run one example
npx vitest run examples/todo-app/
```

Tests use Docker or Podman to spin up a temporary PostgreSQL instance automatically. If you don't have a container runtime, set `TEST_DATABASE_URL` to point at an existing PostgreSQL instance.
