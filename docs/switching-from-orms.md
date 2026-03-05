# Switching from ORM-Managed Migrations

A practical guide for teams migrating from Prisma, TypeORM, Sequelize, Django, Rails/ActiveRecord, or SQLAlchemy/Alembic to schema-flow.

## Why Teams Switch

ORMs are great for writing queries. They're less great for managing PostgreSQL schemas. Here's why teams outgrow ORM-managed migrations:

**PostgreSQL has features your ORM can't express.** Row-level security, partial indexes, GIN indexes on JSONB, custom trigger functions, check constraints with complex expressions, generated columns, materialized views -- these are the features that make PostgreSQL worth using. Most ORMs either don't support them or require you to drop down to raw SQL, defeating the purpose of the abstraction.

**Migration diffs are often wrong.** ORMs generate migrations by comparing your model definitions to the database. This comparison is imperfect. Prisma might generate a `DROP COLUMN` + `ADD COLUMN` when you just renamed something. TypeORM might miss a constraint change entirely. You end up manually editing generated migrations anyway.

**You're fighting the ORM instead of using PostgreSQL.** Want a partial index? Write raw SQL in a migration. Want RLS policies? Raw SQL. Want a trigger function? Raw SQL. At some point, half your migrations are raw SQL escaping the ORM's abstraction layer.

**No drift detection.** If someone modifies the database directly (during an incident, through a database GUI, via another service), your ORM has no idea. The next migration might fail or, worse, silently produce the wrong result.

## What You Gain

### Full PostgreSQL Feature Access

schema-flow supports every PostgreSQL feature as a first-class YAML declaration. No workarounds, no raw SQL escape hatches for structural changes.

**Row-level security with policies:**

```yaml
# schema/tables/documents.yaml
table: documents
rls: true
columns:
  - name: id
    type: serial
    primary_key: true
  - name: owner_id
    type: integer
  - name: content
    type: text
policies:
  - name: owners_only
    for: ALL
    using: "owner_id = current_setting('app.user_id')::int"
```

**Custom trigger functions:**

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

```yaml
# schema/tables/orders.yaml
table: orders
columns:
  - name: id
    type: serial
    primary_key: true
  - name: total
    type: numeric(10,2)
  - name: updated_at
    type: timestamptz
    default: now()
triggers:
  - name: set_orders_updated_at
    timing: BEFORE
    events: [UPDATE]
    function: update_timestamp
    for_each: ROW
```

**Partial indexes, GIN indexes, covering indexes:**

```yaml
indexes:
  # Partial index -- only index active users
  - columns: [email]
    unique: true
    where: "active = true"

  # GIN index on JSONB column
  - columns: [metadata]
    method: gin
    opclass: jsonb_path_ops

  # Expression index
  - columns: ["lower(email)"]
    unique: true

  # Covering index with INCLUDE
  - columns: [status]
    include: [created_at, total]
```

**Check constraints:**

```yaml
columns:
  - name: price
    type: numeric(10,2)
  - name: email
    type: text
checks:
  - name: chk_positive_price
    expression: "price > 0"
  - name: chk_email_format
    expression: "email ~* '^[^@]+@[^@]+$'"
```

**Materialized views:**

```yaml
# schema/views/mv_daily_stats.yaml
materialized_view: daily_stats
query: "SELECT date_trunc('day', created_at) as day, count(*) as total FROM orders GROUP BY 1"
indexes:
  - columns: [day]
    unique: true
```

**Cross-schema foreign keys:**

```yaml
columns:
  - name: tenant_id
    type: integer
    references:
      table: tenants
      column: id
      schema: shared
```

**Declarative role management and grants:**

```yaml
# schema/roles/app_readonly.yaml
role: app_readonly
login: false
```

```yaml
# schema/tables/users.yaml
table: users
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: varchar(255)
grants:
  - to: app_readonly
    privileges: [SELECT]
```

**Expand/contract for zero-downtime renames:**

Instead of renaming a column (which breaks running code during deploy), schema-flow supports the expand/contract pattern:

1. Add the new column alongside the old one with a dual-write trigger
2. Backfill existing rows
3. Update application code to use the new column
4. Contract: drop the old column and trigger

This is built into the CLI -- no manual trigger writing required.

### Readable YAML Over DSL

Compare a Prisma schema:

```prisma
model Order {
  id        Int      @id @default(autoincrement())
  status    String   @default("pending")
  total     Decimal  @db.Decimal(10, 2)
  userId    Int
  user      User     @relation(fields: [userId], references: [id])
  createdAt DateTime @default(now()) @map("created_at")

  @@index([status])
  @@map("orders")
}
```

With the schema-flow equivalent:

```yaml
table: orders
columns:
  - name: id
    type: serial
    primary_key: true
  - name: status
    type: varchar(20)
    default: "'pending'"
  - name: total
    type: numeric(10,2)
  - name: user_id
    type: integer
    references:
      table: users
      column: id
  - name: created_at
    type: timestamptz
    default: now()
indexes:
  - columns: [status]
```

The YAML maps directly to PostgreSQL concepts. No `@map`, no `@db.Decimal`, no guessing what the ORM will generate. What you write is what you get.

### Drift Detection

```bash
npx @mabulu-inc/schema-flow drift
```

This compares your YAML definitions against the live database and reports every difference: missing columns, extra indexes, attribute mismatches on roles, missing grants. If your staging database drifted from what your YAML describes, you'll know before deploying.

## Step-by-Step Migration

### 1. Generate YAML from Your Existing Database

```bash
DATABASE_URL="postgresql://user:pass@localhost/myapp" npx @mabulu-inc/schema-flow generate
```

This introspects your database and creates YAML files for every table, enum, function, view, and role. The output matches your live database exactly.

### 2. Baseline

```bash
DATABASE_URL="postgresql://user:pass@localhost/myapp" npx @mabulu-inc/schema-flow baseline
```

This tells schema-flow that the current database state matches the YAML. No migrations will be generated for existing objects.

### 3. Verify

```bash
npx @mabulu-inc/schema-flow plan
```

This should show zero pending operations. If it shows changes, your YAML doesn't perfectly match the database -- adjust and re-run until it's clean.

### 4. Start Making Changes in YAML

From here forward, schema changes go through YAML files. Your ORM still handles queries -- schema-flow only manages the schema.

## You Can Keep Your ORM for Queries

schema-flow does not replace your ORM's query builder. It replaces your ORM's migration system.

**Before:** ORM handles both queries AND schema migrations.
**After:** ORM handles queries. schema-flow handles schema migrations.

This means:
- Prisma: Keep using Prisma Client for queries. Stop using `prisma migrate` and `prisma db push`. Run `prisma generate` after schema-flow migrations to update the client.
- TypeORM: Keep using TypeORM repositories and query builder. Stop using `synchronize: true` and TypeORM migrations. You may need to keep your entity decorators in sync with your YAML, or use TypeORM without decorator-based schema syncing.
- Sequelize: Keep using Sequelize models for queries. Stop using `sequelize.sync()` and Sequelize migrations.
- Django: Keep using Django ORM for queries. Stop using `makemigrations` and `migrate`. Update your models to match the YAML when needed.
- Rails: Keep using ActiveRecord for queries. Stop using `rails db:migrate`. Update your models if they rely on schema introspection.
- SQLAlchemy: Keep using SQLAlchemy for queries. Stop using Alembic.

## Handling the ORM's Migration Table

Your ORM probably created a migration tracking table:

| ORM | Table |
|---|---|
| Prisma | `_prisma_migrations` |
| TypeORM | `migrations` |
| Sequelize | `SequelizeMeta` |
| Django | `django_migrations` |
| Rails | `schema_migrations`, `ar_internal_metadata` |
| Alembic | `alembic_version` |

You have two options:

**Option A: Leave it alone.** schema-flow ignores tables it doesn't know about. The old migration table will sit there harmlessly. This is the simplest approach.

**Option B: Clean it up.** After you've fully switched to schema-flow and confirmed everything works, drop the old table:

```sql
-- In a post script
DROP TABLE IF EXISTS _prisma_migrations;
```

Either way, it won't affect schema-flow's operation.

## Feature Comparison

| Feature | Prisma | TypeORM | Sequelize | schema-flow |
|---|---|---|---|---|
| Row-level security (RLS) | No | No | No | Yes |
| RLS policies | No | No | No | Yes |
| Custom trigger functions | Manual SQL | Manual SQL | Manual SQL | YAML |
| Partial indexes | Manual SQL | Limited | No | YAML |
| GIN/GiST indexes | Manual SQL | Decorator | No | YAML |
| Expression indexes | No | No | No | YAML |
| Covering indexes (INCLUDE) | No | No | No | YAML |
| Check constraints | Limited | Decorator | No | YAML |
| Materialized views | No | No | No | YAML |
| Views | No | Decorator | No | YAML |
| Cross-schema foreign keys | No | No | No | YAML |
| Declarative roles | No | No | No | YAML |
| Table grants (GRANT/REVOKE) | No | No | No | YAML |
| Function grants | No | No | No | YAML |
| Enums (add/reorder values) | Limited | Limited | Limited | YAML |
| Drift detection | No | No | No | Yes |
| Expand/contract renames | No | No | No | Yes |
| Mixins (shared column sets) | No | No | No | YAML |
| Destructive operation blocking | No | No | No | Yes |
| Pre/post SQL scripts | Manual | Manual | Manual | Built-in |
| Seed data | Manual | Manual | Separate | YAML |

## Common Concerns

### "My ORM auto-generates migration files. This seems like more work."

It's less work. With an ORM, you: modify the model, generate a migration, review the generated SQL (because it's often wrong), manually edit it, test it, commit both files.

With schema-flow, you: edit the YAML file, run `plan` to verify, commit. One file, one step.

### "How do I keep my ORM models in sync with the YAML?"

This depends on your ORM:

- **Prisma:** Run `prisma db pull` after a schema-flow migration to update your `schema.prisma`, then run `prisma generate` to update the client. Or maintain your Prisma schema manually alongside the YAML.
- **TypeORM:** Update your entity decorators to match the YAML. With `synchronize: false`, TypeORM won't try to change the database.
- **Sequelize:** Update your model definitions to match. With no `sync()` calls, Sequelize just uses whatever's in the database.
- **Django:** Update your Django models. With no `migrate` calls, Django just queries the existing schema.

### "What about rollbacks?"

```bash
npx @mabulu-inc/schema-flow down
```

This reverses the most recent migration. Unlike ORM-generated rollbacks (which are often incomplete or missing), schema-flow tracks exactly what was applied and generates the correct reverse SQL.

### "We have hundreds of existing ORM migrations. Do they all get thrown away?"

No. Your existing migrations already ran and shaped the database. The `generate` + `baseline` steps capture the current state of the database as YAML. Your old migration files can stay in the repo for historical reference, or you can archive them. They won't interfere with schema-flow.

### "What about data migrations?"

Use pre and post SQL scripts:

```bash
npx @mabulu-inc/schema-flow new pre backfill_user_names
```

This creates a timestamped SQL file. Write your data migration in plain SQL. It runs once, in order, and is tracked alongside schema migrations:

```sql
-- schema/pre/20260305120000_backfill_user_names.sql
UPDATE users SET display_name = first_name || ' ' || last_name
WHERE display_name IS NULL;
```

### "We use Prisma's type generation. Will that still work?"

Yes. After schema-flow runs a migration, run `prisma db pull` followed by `prisma generate`. Your Prisma Client types will update to match the new schema. You can add this to your post-migration script or CI pipeline.

## Project Structure After Migration

```
myproject/
  schema/
    tables/
      users.yaml
      orders.yaml
      products.yaml
    enums/
      order_status.yaml
    functions/
      update_timestamp.yaml
    views/
      active_users.yaml
      daily_stats.yaml       # materialized view
    roles/
      app_readonly.yaml
      app_service.yaml
    mixins/
      timestamps.yaml        # shared created_at + updated_at columns
    pre/
      20260305120000_backfill_names.sql
    post/
      20260305120001_seed_roles.sql
  prisma/                    # or your ORM's config -- still used for queries
    schema.prisma
  src/
    ...
```

## Quick Reference

| ORM Command | schema-flow Equivalent |
|---|---|
| `prisma migrate dev` | `schema-flow run` |
| `prisma migrate deploy` | `schema-flow run` |
| `prisma db push` | `schema-flow run` |
| `prisma db pull` | `schema-flow generate` |
| `npx typeorm migration:generate` | Edit the table's YAML file |
| `npx typeorm migration:run` | `schema-flow run` |
| `npx sequelize-cli db:migrate` | `schema-flow run` |
| `python manage.py migrate` | `schema-flow run` |
| `rails db:migrate` | `schema-flow run` |
| `alembic upgrade head` | `schema-flow run` |
| *(no equivalent)* | `schema-flow drift` |
| *(no equivalent)* | `schema-flow plan` (always shows what would run) |
