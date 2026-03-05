# Switching from Imperative Migration Tools

A practical guide for teams migrating from Knex, Flyway, Liquibase, golang-migrate, dbmate, or raw SQL migration files to schema-flow.

::: tip Prerequisites
schema-flow is on GitHub Packages. See [Getting Started](./getting-started#configure-the-registry) for registry setup.
:::

## Why Teams Switch

After a year or two with imperative migrations, most teams hit the same pain points:

**Migration debt accumulates.** You have 200+ migration files. Nobody reads them. The only way to understand your current schema is to look at the database itself, or mentally replay every migration from the beginning.

**You can't see what your schema looks like.** Your migration files describe *changes*, not *state*. Want to know what columns are on the `orders` table? Read through every migration that ever touched it and piece it together.

**Merge conflicts are constant.** Two developers add migrations at the same time. Both get sequence number `20260305120000`. Now someone has to renumber, retest, and hope the ordering still works.

**There's no drift detection.** Someone ran `ALTER TABLE` in production to fix an outage. Nobody updated the migration files. Now your migrations think the schema looks one way, but the database disagrees. You won't find out until the next deploy fails.

## The Mental Shift

Imperative migrations say: *"Here's how to get from state A to state B."*

schema-flow says: *"Here's what the schema should look like. Figure out the SQL."*

This is the same shift as writing Dockerfiles vs. manually SSHing into servers, or Terraform vs. clicking through the AWS console. You describe the desired end state, and the tool computes the diff.

With imperative migrations:

```sql
-- 20260301120000_add_status_to_orders.sql
ALTER TABLE orders ADD COLUMN status varchar(20) DEFAULT 'pending';
CREATE INDEX idx_orders_status ON orders (status);

-- 20260303090000_add_shipped_at_to_orders.sql
ALTER TABLE orders ADD COLUMN shipped_at timestamptz;
```

With schema-flow, you just describe the table as it should exist:

```yaml
# schema/tables/orders.yaml
table: orders
columns:
  - name: id
    type: serial
    primary_key: true
  - name: customer_id
    type: integer
  - name: status
    type: varchar(20)
    default: "'pending'"
  - name: shipped_at
    type: timestamptz
    nullable: true
  - name: created_at
    type: timestamptz
    default: now()
indexes:
  - columns: [status]
```

schema-flow compares this YAML to your live database and generates the exact `ALTER` statements needed. Add a column? Add a line to the YAML. Remove an index? Delete a line. Rename a column? Use the expand/contract workflow for zero downtime.

## Step-by-Step Migration

### 1. Generate YAML from Your Existing Database

Point schema-flow at your running database and it will produce YAML files that exactly match what's already there:

```bash
DATABASE_URL="postgresql://user:pass@localhost/myapp" npx @mabulu-inc/schema-flow generate
```

This creates one YAML file per table, enum, function, view, and role in your `schema/` directory. The output matches your live database exactly -- no changes will be applied.

### 2. Baseline to Mark Everything as Applied

Tell schema-flow that the current database state matches the YAML. This records a baseline so schema-flow won't try to recreate tables that already exist:

```bash
DATABASE_URL="postgresql://user:pass@localhost/myapp" npx @mabulu-inc/schema-flow baseline
```

At this point, running `schema-flow plan` should show zero pending operations.

### 3. Start Writing YAML

From here on, all schema changes go through YAML files. Want to add a column?

```yaml
# Just add the new column to the existing YAML file
columns:
  # ... existing columns ...
  - name: phone
    type: varchar(20)
    nullable: true
```

Preview what SQL schema-flow will generate:

```bash
npx @mabulu-inc/schema-flow plan
```

Output:

```
[plan] ALTER TABLE "public"."users" ADD COLUMN "phone" varchar(20);
```

Apply it:

```bash
npx @mabulu-inc/schema-flow run
```

### 4. Use Pre/Post Scripts for Procedural Work

Data migrations, backfills, and one-off operations don't belong in YAML. They belong in pre and post SQL scripts, which run before and after the schema migration:

```bash
# Generate a timestamped script
npx @mabulu-inc/schema-flow new pre backfill_phone_numbers
npx @mabulu-inc/schema-flow new post seed_default_roles
```

This creates files like:

```
schema/pre/20260305120000_backfill_phone_numbers.sql
schema/post/20260305120001_seed_default_roles.sql
```

Write your SQL directly:

```sql
-- schema/pre/20260305120000_backfill_phone_numbers.sql
UPDATE users SET phone = legacy_phone FROM user_profiles
WHERE users.id = user_profiles.user_id AND users.phone IS NULL;
```

Pre scripts run before the migration (useful for preparing data). Post scripts run after (useful for seeding or cleanup). Both are tracked so they only run once.

### 5. Stop Writing Migration Files

You can keep your old migration files around for reference if you want. They won't interfere with schema-flow. But from this point forward, the YAML files are your source of truth.

## What Stays the Same

**You still write SQL for data migrations.** schema-flow handles structural changes (tables, columns, indexes, constraints). Data transformations still use SQL, just in pre/post scripts instead of migration files.

**You still review changes before applying.** The `--dry-run` flag shows you exactly what SQL will run. Nothing happens until you approve it.

**Your deployment pipeline structure stays similar.** You still have a "run migrations" step. It just calls `schema-flow run` instead of `knex migrate:latest` or `flyway migrate`.

## What Changes

**One file per table, not one file per change.** Instead of 15 migration files that each touch the `users` table, you have one `users.yaml` that shows the full current state.

**schema-flow computes the ALTER statements.** You don't manually write `ALTER TABLE ADD COLUMN` anymore. You add the column to the YAML, and schema-flow figures out the SQL.

**Destructive operations are blocked by default.** Dropping a column or table requires the `--allow-destructive` flag. You won't accidentally destroy data.

**Drift detection catches divergence.** Run `schema-flow drift` to compare your YAML against the live database. If someone made a manual change, you'll know immediately.

## Common Concerns

### "What about rollbacks?"

schema-flow tracks every migration and can reverse it:

```bash
npx @mabulu-inc/schema-flow down
```

This generates and runs the reverse SQL for the most recent migration. Columns that were added get dropped, indexes that were created get removed.

### "What about data migrations?"

Use pre and post scripts. Pre scripts run raw SQL before the schema migration. Post scripts run after. They're tracked and only execute once, just like your current migration files:

```
schema/
  pre/
    20260305120000_backfill_emails.sql
  post/
    20260305120001_seed_roles.sql
  tables/
    users.yaml
    roles.yaml
```

The execution order is always: pre scripts, then schema migration, then post scripts.

### "What about CI/CD?"

A typical pipeline:

```yaml
# In your CI pipeline (PR checks)
- run: npx @mabulu-inc/schema-flow plan
  # Shows what SQL would run, fails if there are destructive operations

# In your CD pipeline (deploy)
- run: npx @mabulu-inc/schema-flow run
```

Use `--dry-run` in PR checks so reviewers can see what SQL will run. Run the actual migration in your deploy step.

### "What about team coordination?"

Two developers both modify `users.yaml` in different branches. Git handles the merge conflict in the YAML file the same way it handles any other code conflict -- you resolve it, and schema-flow computes the correct SQL from the merged result.

This is a significant improvement over imperative migrations, where two developers adding separate migrations can create ordering conflicts that are much harder to resolve.

For extra safety, run `schema-flow drift` in CI to verify that your YAML matches what's actually in the database.

### "Can I still write raw SQL?"

Yes. Pre and post scripts are plain SQL files. schema-flow runs them in timestamp order and tracks which ones have been executed. You have full control over what SQL runs.

### "What if schema-flow generates the wrong SQL?"

Run `--dry-run` first. Always. The plan output shows every SQL statement that will execute. If something looks wrong, adjust the YAML and re-plan.

### "Can I migrate incrementally?"

Yes. You don't have to convert your entire project at once. Generate YAML from your existing database, baseline it, and start using schema-flow for new changes. Your old migration files can stay in the repo -- they just won't be used anymore.

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
    roles/
      app_readonly.yaml
    mixins/
      timestamps.yaml
    pre/
      20260305120000_backfill_emails.sql
    post/
      20260305120001_seed_roles.sql
  migrations/           # your old migration files (optional, for reference)
    20240101_create_users.sql
    20240115_add_orders.sql
    ...
```

## Quick Reference

| Imperative Tool Command | schema-flow Equivalent |
|---|---|
| `knex migrate:make add_column` | Edit the table's YAML file |
| `knex migrate:latest` | `schema-flow run` |
| `knex migrate:rollback` | `schema-flow down` |
| `flyway migrate` | `schema-flow run` |
| `flyway info` | `schema-flow plan` |
| `dbmate up` | `schema-flow run` |
| `dbmate new` | `schema-flow new pre` or edit YAML |
| *(no equivalent)* | `schema-flow drift` |
| *(no equivalent)* | `schema-flow generate` |
