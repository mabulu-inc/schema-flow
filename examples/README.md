# Examples

Three complete examples, each with a full schema and integration tests. They progress from simple to complex — start with **todo-app** to learn the basics, then explore **cms** and **multi-tenant-orders** for advanced features.

## [Todo App](./todo-app/)

A single-user todo list — the simplest starting point.

**Features covered:** tables, enums, foreign keys, indexes, check constraints, mixins, trigger functions, defaults.

## [CMS](./cms/)

A multi-role content management system.

**Features covered:** everything in todo-app, plus extensions, roles, RLS policies, views, materialized views, seed data, soft delete, comments on all object types, GIN/trigram indexes, SECURITY DEFINER functions, table and column grants.

## [Multi-Tenant Orders](./multi-tenant-orders/)

A multi-tenant order system with membership-based access control.

**Features covered:** everything in cms, plus tenant isolation via RESTRICTIVE RLS policies, modular roles (multiple roles per user per tenant), security functions for API integration (`register_account`, `create_membership`, `begin_session`, `grant_role`, `revoke_role`), generated columns, column-level grants, audit trail.

## Running the tests

Every example includes an integration test (`example.test.ts`) that migrates a fresh database and verifies the application behavior end-to-end. Run them all:

```bash
npx vitest run examples/
```

Or run a single example:

```bash
npx vitest run examples/todo-app/
```

Tests use Docker or Podman to spin up a temporary PostgreSQL instance automatically — no manual setup required. If you don't have a container runtime, set `TEST_DATABASE_URL` to point at an existing PostgreSQL instance.

## Using the test infrastructure

The examples use the public `@mabulu-inc/schema-flow/testing` module:

```typescript
import { resolveConfig, runAll, closePool } from "@mabulu-inc/schema-flow";
import { createTestDb, execSql, withConnection } from "@mabulu-inc/schema-flow/testing";
```

See any `example.test.ts` for the full pattern.
