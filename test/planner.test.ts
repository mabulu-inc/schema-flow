// test/planner.test.ts
// Integration tests for the diff/planning engine

import { describe, it, expect } from "vitest";
import { useTestClient } from "./helpers.js";
import { buildPlan } from "../src/planner/index.js";
import type { TableSchema } from "../src/schema/types.js";

describe("planner", () => {
  const ctx = useTestClient();

  it("plans CREATE TABLE for a new table", async () => {
    const desired: TableSchema[] = [
      {
        table: "users",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "email", type: "varchar(255)" },
        ],
      },
    ];

    const plan = await buildPlan(ctx.client, desired, "public");
    expect(plan.summary.tablesToCreate).toContain("users");
    expect(plan.structureOps.length).toBeGreaterThan(0);
    expect(plan.structureOps[0].type).toBe("create_table");
    expect(plan.structureOps[0].sql).toContain("CREATE TABLE");
    expect(plan.structureOps[0].destructive).toBe(false);
  });

  it("separates foreign keys into the FK phase", async () => {
    const desired: TableSchema[] = [
      {
        table: "authors",
        columns: [{ name: "id", type: "serial", primary_key: true }],
      },
      {
        table: "books",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          {
            name: "author_id",
            type: "integer",
            references: { table: "authors", column: "id", on_delete: "CASCADE" },
          },
        ],
      },
    ];

    const plan = await buildPlan(ctx.client, desired, "public");

    // Structure ops should NOT contain FK
    const structureSql = plan.structureOps.map((o) => o.sql).join("\n");
    expect(structureSql).not.toContain("FOREIGN KEY");

    // FK phase should have the constraint with NOT VALID
    expect(plan.foreignKeyOps.length).toBeGreaterThan(0);
    expect(plan.foreignKeyOps[0].sql).toContain("FOREIGN KEY");
    expect(plan.foreignKeyOps[0].sql).toContain("CASCADE");
    expect(plan.foreignKeyOps[0].sql).toContain("NOT VALID");

    // Validate phase should have VALIDATE CONSTRAINT
    expect(plan.validateOps.length).toBeGreaterThan(0);
  });

  it("plans ADD COLUMN for a new column on an existing table", async () => {
    await ctx.client.query(`CREATE TABLE users (id serial PRIMARY KEY)`);

    const desired: TableSchema[] = [
      {
        table: "users",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "email", type: "varchar(255)" },
        ],
      },
    ];

    const plan = await buildPlan(ctx.client, desired, "public");
    const addColOp = plan.structureOps.find((o) => o.type === "add_column");
    expect(addColOp).toBeDefined();
    expect(addColOp!.sql).toContain("ADD COLUMN");
    expect(addColOp!.sql).toContain("email");
    expect(addColOp!.destructive).toBe(false);
  });

  it("blocks DROP COLUMN by default (safe mode)", async () => {
    await ctx.client.query(`CREATE TABLE users (id serial PRIMARY KEY, name text NOT NULL, email text NOT NULL)`);

    const desired: TableSchema[] = [
      {
        table: "users",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "email", type: "text" },
        ],
      },
    ];

    // Default: safe mode (no allowDestructive)
    const plan = await buildPlan(ctx.client, desired, "public");

    // The drop_column should be BLOCKED, not in operations
    const dropOp = plan.operations.find((o) => o.type === "drop_column");
    expect(dropOp).toBeUndefined();

    // But it should appear in blocked list
    expect(plan.blocked).toHaveLength(1);
    expect(plan.blocked[0].type).toBe("drop_column");
    expect(plan.blocked[0].sql).toContain("DROP COLUMN");
    expect(plan.blocked[0].sql).toContain("name");
    expect(plan.blocked[0].destructive).toBe(true);

    expect(plan.summary.blockedCount).toBe(1);
  });

  it("allows DROP COLUMN with allowDestructive flag", async () => {
    await ctx.client.query(`CREATE TABLE users (id serial PRIMARY KEY, name text NOT NULL, email text NOT NULL)`);

    const desired: TableSchema[] = [
      {
        table: "users",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "email", type: "text" },
        ],
      },
    ];

    const plan = await buildPlan(ctx.client, desired, "public", { allowDestructive: true });

    const dropOp = plan.structureOps.find((o) => o.type === "drop_column");
    expect(dropOp).toBeDefined();
    expect(dropOp!.sql).toContain("DROP COLUMN");
    expect(dropOp!.sql).toContain("name");
    expect(dropOp!.destructive).toBe(true);

    // Nothing should be blocked
    expect(plan.blocked).toHaveLength(0);
    expect(plan.summary.blockedCount).toBe(0);
    expect(plan.summary.destructiveCount).toBe(1);
  });

  it("plans ALTER COLUMN for type changes", async () => {
    await ctx.client.query(`CREATE TABLE products (id serial PRIMARY KEY, price integer NOT NULL)`);

    const desired: TableSchema[] = [
      {
        table: "products",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "price", type: "numeric(10,2)" },
        ],
      },
    ];

    // This is a narrowing type change (integer → numeric) — blocked by default
    const safePlan = await buildPlan(ctx.client, desired, "public");
    expect(safePlan.blocked.length).toBeGreaterThanOrEqual(1);

    // With allowDestructive, it goes through
    const plan = await buildPlan(ctx.client, desired, "public", { allowDestructive: true });
    const alterOp = plan.structureOps.find((o) => o.type === "alter_column");
    expect(alterOp).toBeDefined();
    expect(alterOp!.sql).toContain("TYPE numeric(10,2)");
  });

  it("allows safe type widening without --allow-destructive", async () => {
    await ctx.client.query(`CREATE TABLE metrics (id serial PRIMARY KEY, count integer NOT NULL)`);

    const desired: TableSchema[] = [
      {
        table: "metrics",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "count", type: "bigint" },
        ],
      },
    ];

    // integer → bigint is a safe widening
    const plan = await buildPlan(ctx.client, desired, "public");
    const alterOp = plan.structureOps.find((o) => o.type === "alter_column" && o.sql.includes("TYPE bigint"));
    expect(alterOp).toBeDefined();
    expect(alterOp!.destructive).toBe(false);
    expect(plan.blocked).toHaveLength(0);
  });

  it("SET NOT NULL uses safe 4-step pattern (not blocked)", async () => {
    await ctx.client.query(`CREATE TABLE items (id serial PRIMARY KEY, name text)`);

    const desired: TableSchema[] = [
      {
        table: "items",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "name", type: "text" }, // nullable=false by convention → NOT NULL
        ],
      },
    ];

    const plan = await buildPlan(ctx.client, desired, "public");

    // SET NOT NULL should NOT be blocked — it uses the safe 4-step pattern
    const setNotNullBlocked = plan.blocked.find((o) => o.sql.includes("SET NOT NULL"));
    expect(setNotNullBlocked).toBeUndefined();

    // Should have a CHECK NOT VALID in structure phase
    const checkOp = plan.structureOps.find((o) => o.type === "add_check_not_valid");
    expect(checkOp).toBeDefined();
    expect(checkOp!.destructive).toBe(false);
  });

  it("allows DROP NOT NULL as safe (widening nullability)", async () => {
    await ctx.client.query(`CREATE TABLE items (id serial PRIMARY KEY, name text NOT NULL)`);

    const desired: TableSchema[] = [
      {
        table: "items",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "name", type: "text", nullable: true },
        ],
      },
    ];

    const plan = await buildPlan(ctx.client, desired, "public");

    // DROP NOT NULL is safe — should be in operations, not blocked
    const dropNotNull = plan.structureOps.find((o) => o.sql.includes("DROP NOT NULL"));
    expect(dropNotNull).toBeDefined();
    expect(dropNotNull!.destructive).toBe(false);
    expect(plan.blocked).toHaveLength(0);
  });

  it("produces empty plan when schema matches", async () => {
    await ctx.client.query(`CREATE TABLE simple (id serial PRIMARY KEY, name text NOT NULL)`);

    const desired: TableSchema[] = [
      {
        table: "simple",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "name", type: "text" },
        ],
      },
    ];

    const plan = await buildPlan(ctx.client, desired, "public");
    expect(plan.operations).toHaveLength(0);
    expect(plan.blocked).toHaveLength(0);
  });

  it("plans create_trigger for new table with triggers", async () => {
    const desired: TableSchema[] = [
      {
        table: "items",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "updated_at", type: "timestamptz" },
        ],
        triggers: [
          {
            name: "set_items_updated",
            timing: "BEFORE",
            events: ["UPDATE"],
            function: "update_timestamp",
            for_each: "ROW",
          },
        ],
      },
    ];

    // Create the function first
    await ctx.client.query(`
      CREATE OR REPLACE FUNCTION update_timestamp() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN NEW.updated_at = now(); RETURN NEW; END; $$
    `);

    const plan = await buildPlan(ctx.client, desired, "public");
    const triggerOp = plan.structureOps.find((o) => o.type === "create_trigger");
    expect(triggerOp).toBeDefined();
    expect(triggerOp!.sql).toContain("CREATE TRIGGER");
    expect(triggerOp!.sql).toContain("set_items_updated");
    expect(triggerOp!.destructive).toBe(false);
  });

  it("plans drop_trigger for removed trigger (destructive)", async () => {
    await ctx.client.query(`CREATE TABLE test_drop_trig (id serial PRIMARY KEY, val text)`);
    await ctx.client.query(`
      CREATE OR REPLACE FUNCTION noop_trigger() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN RETURN NEW; END; $$
    `);
    await ctx.client.query(`
      CREATE TRIGGER old_trigger BEFORE UPDATE ON test_drop_trig
      FOR EACH ROW EXECUTE FUNCTION noop_trigger()
    `);

    // Desired has no triggers
    const desired: TableSchema[] = [
      {
        table: "test_drop_trig",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "val", type: "text" },
        ],
      },
    ];

    const plan = await buildPlan(ctx.client, desired, "public");
    // Drop should be blocked (destructive)
    expect(plan.blocked.length).toBeGreaterThanOrEqual(1);
    const dropOp = plan.blocked.find((o) => o.type === "drop_trigger");
    expect(dropOp).toBeDefined();
    expect(dropOp!.destructive).toBe(true);
  });

  it("plans safe trigger replacement when trigger definition changes", async () => {
    await ctx.client.query(`CREATE TABLE test_replace_trig (id serial PRIMARY KEY, val text)`);
    await ctx.client.query(`
      CREATE OR REPLACE FUNCTION noop_trigger() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN RETURN NEW; END; $$
    `);
    await ctx.client.query(`
      CREATE TRIGGER my_trigger BEFORE UPDATE ON test_replace_trig
      FOR EACH ROW EXECUTE FUNCTION noop_trigger()
    `);

    // Desired has the same trigger name but AFTER instead of BEFORE
    const desired: TableSchema[] = [
      {
        table: "test_replace_trig",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "val", type: "text" },
        ],
        triggers: [
          {
            name: "my_trigger",
            timing: "AFTER",
            events: ["UPDATE"],
            function: "noop_trigger",
            for_each: "ROW",
          },
        ],
      },
    ];

    const plan = await buildPlan(ctx.client, desired, "public");
    // Should have drop + create, both non-destructive (safe replacement)
    const dropOp = plan.structureOps.find((o) => o.type === "drop_trigger");
    const createOp = plan.structureOps.find((o) => o.type === "create_trigger");
    expect(dropOp).toBeDefined();
    expect(createOp).toBeDefined();
    expect(dropOp!.destructive).toBe(false);
    expect(createOp!.destructive).toBe(false);
    // Nothing blocked
    expect(plan.blocked.filter((o) => o.type === "drop_trigger")).toHaveLength(0);
  });

  it("produces empty plan when triggers match", async () => {
    await ctx.client.query(`CREATE TABLE matched_trig (id serial PRIMARY KEY, val text NOT NULL)`);
    await ctx.client.query(`
      CREATE OR REPLACE FUNCTION noop_trigger() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN RETURN NEW; END; $$
    `);
    await ctx.client.query(`
      CREATE TRIGGER my_trigger BEFORE UPDATE ON matched_trig
      FOR EACH ROW EXECUTE FUNCTION noop_trigger()
    `);

    const desired: TableSchema[] = [
      {
        table: "matched_trig",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "val", type: "text" },
        ],
        triggers: [
          {
            name: "my_trigger",
            timing: "BEFORE",
            events: ["UPDATE"],
            function: "noop_trigger",
            for_each: "ROW",
          },
        ],
      },
    ];

    const plan = await buildPlan(ctx.client, desired, "public");
    expect(plan.operations).toHaveLength(0);
    expect(plan.blocked).toHaveLength(0);
  });

  it("plans enable_rls and create_policy for new table with RLS", async () => {
    const desired: TableSchema[] = [
      {
        table: "orders",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "user_id", type: "integer" },
        ],
        rls: true,
        force_rls: true,
        policies: [
          {
            name: "users_see_own",
            for: "SELECT",
            using: "user_id = 1",
            permissive: true,
          },
        ],
      },
    ];

    const plan = await buildPlan(ctx.client, desired, "public");
    const rlsOps = plan.structureOps.filter((o) => o.type === "enable_rls");
    expect(rlsOps.length).toBe(2); // enable + force
    expect(rlsOps[0].sql).toContain("ENABLE ROW LEVEL SECURITY");
    expect(rlsOps[1].sql).toContain("FORCE ROW LEVEL SECURITY");
    expect(rlsOps.every((o) => !o.destructive)).toBe(true);

    const policyOps = plan.structureOps.filter((o) => o.type === "create_policy");
    expect(policyOps).toHaveLength(1);
    expect(policyOps[0].sql).toContain("CREATE POLICY");
    expect(policyOps[0].sql).toContain("users_see_own");
    expect(policyOps[0].destructive).toBe(false);
  });

  it("plans enable_rls for existing table gaining RLS", async () => {
    await ctx.client.query(`CREATE TABLE items (id serial PRIMARY KEY)`);

    const desired: TableSchema[] = [
      {
        table: "items",
        columns: [{ name: "id", type: "serial", primary_key: true }],
        rls: true,
      },
    ];

    const plan = await buildPlan(ctx.client, desired, "public");
    const rlsOp = plan.structureOps.find((o) => o.type === "enable_rls");
    expect(rlsOp).toBeDefined();
    expect(rlsOp!.sql).toContain("ENABLE ROW LEVEL SECURITY");
    expect(rlsOp!.destructive).toBe(false);
  });

  it("blocks disable_rls as destructive", async () => {
    await ctx.client.query(`CREATE TABLE rls_table (id serial PRIMARY KEY)`);
    await ctx.client.query(`ALTER TABLE rls_table ENABLE ROW LEVEL SECURITY`);

    const desired: TableSchema[] = [
      {
        table: "rls_table",
        columns: [{ name: "id", type: "serial", primary_key: true }],
        // No rls: true → wants to disable
      },
    ];

    const plan = await buildPlan(ctx.client, desired, "public");
    const blocked = plan.blocked.filter((o) => o.type === "disable_rls");
    expect(blocked.length).toBeGreaterThanOrEqual(1);
    expect(blocked[0].destructive).toBe(true);
  });

  it("plans new policy as non-destructive create_policy", async () => {
    await ctx.client.query(`CREATE TABLE pol_table (id serial PRIMARY KEY, user_id integer NOT NULL)`);
    await ctx.client.query(`ALTER TABLE pol_table ENABLE ROW LEVEL SECURITY`);

    const desired: TableSchema[] = [
      {
        table: "pol_table",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "user_id", type: "integer" },
        ],
        rls: true,
        policies: [
          {
            name: "new_policy",
            for: "SELECT",
            using: "user_id = 1",
            permissive: true,
          },
        ],
      },
    ];

    const plan = await buildPlan(ctx.client, desired, "public");
    const createOp = plan.structureOps.find((o) => o.type === "create_policy");
    expect(createOp).toBeDefined();
    expect(createOp!.destructive).toBe(false);
  });

  it("plans safe policy replacement when definition changes", async () => {
    await ctx.client.query(`CREATE TABLE pol_replace (id serial PRIMARY KEY, user_id integer NOT NULL)`);
    await ctx.client.query(`ALTER TABLE pol_replace ENABLE ROW LEVEL SECURITY`);
    await ctx.client.query(`CREATE POLICY my_pol ON pol_replace FOR SELECT USING (user_id = 1)`);

    const desired: TableSchema[] = [
      {
        table: "pol_replace",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "user_id", type: "integer" },
        ],
        rls: true,
        policies: [
          {
            name: "my_pol",
            for: "ALL", // changed from SELECT
            using: "user_id = 1",
            permissive: true,
          },
        ],
      },
    ];

    const plan = await buildPlan(ctx.client, desired, "public");
    const dropOp = plan.structureOps.find((o) => o.type === "drop_policy");
    const createOp = plan.structureOps.find((o) => o.type === "create_policy");
    expect(dropOp).toBeDefined();
    expect(createOp).toBeDefined();
    expect(dropOp!.destructive).toBe(false);
    expect(createOp!.destructive).toBe(false);
  });

  it("blocks removed policy as destructive drop_policy", async () => {
    await ctx.client.query(`CREATE TABLE pol_remove (id serial PRIMARY KEY)`);
    await ctx.client.query(`ALTER TABLE pol_remove ENABLE ROW LEVEL SECURITY`);
    await ctx.client.query(`CREATE POLICY old_pol ON pol_remove FOR SELECT USING (true)`);

    const desired: TableSchema[] = [
      {
        table: "pol_remove",
        columns: [{ name: "id", type: "serial", primary_key: true }],
        rls: true,
        // No policies → old_pol should be dropped (destructive)
      },
    ];

    const plan = await buildPlan(ctx.client, desired, "public");
    const blocked = plan.blocked.filter((o) => o.type === "drop_policy");
    expect(blocked).toHaveLength(1);
    expect(blocked[0].destructive).toBe(true);
  });

  it("produces empty plan when RLS and policies match", async () => {
    await ctx.client.query(`CREATE TABLE rls_match (id serial PRIMARY KEY, user_id integer NOT NULL)`);
    await ctx.client.query(`ALTER TABLE rls_match ENABLE ROW LEVEL SECURITY`);
    await ctx.client.query(`CREATE POLICY match_pol ON rls_match FOR SELECT USING (user_id = 1)`);

    const desired: TableSchema[] = [
      {
        table: "rls_match",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "user_id", type: "integer" },
        ],
        rls: true,
        policies: [
          {
            name: "match_pol",
            for: "SELECT",
            using: "(user_id = 1)",
            permissive: true,
          },
        ],
      },
    ];

    const plan = await buildPlan(ctx.client, desired, "public");
    const rlsOps = plan.operations.filter((o) => o.type === "enable_rls" || o.type === "disable_rls");
    const policyOps = plan.operations.filter((o) => o.type === "create_policy" || o.type === "drop_policy");
    expect(rlsOps).toHaveLength(0);
    expect(policyOps).toHaveLength(0);
  });

  // ─── ZDM Feature Tests ──────────────────────────────────────────────────

  it("FK ops produce NOT VALID + VALIDATE two-step", async () => {
    const desired: TableSchema[] = [
      {
        table: "authors",
        columns: [{ name: "id", type: "serial", primary_key: true }],
      },
      {
        table: "books",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          {
            name: "author_id",
            type: "integer",
            references: { table: "authors", column: "id", on_delete: "CASCADE" },
          },
        ],
      },
    ];

    const plan = await buildPlan(ctx.client, desired, "public");

    // FK phase should have NOT VALID
    const fkOps = plan.foreignKeyOps;
    expect(fkOps.length).toBeGreaterThan(0);
    expect(fkOps[0].sql).toContain("NOT VALID");
    expect(fkOps[0].type).toBe("add_foreign_key_not_valid");

    // Validate phase should have VALIDATE CONSTRAINT
    const validateOps = plan.validateOps;
    expect(validateOps.length).toBeGreaterThan(0);
    const validateFk = validateOps.find(
      (o) => o.type === "validate_constraint" && o.sql.includes("fk_books_author_id_authors"),
    );
    expect(validateFk).toBeDefined();
    expect(validateFk!.sql).toContain("VALIDATE CONSTRAINT");
  });

  it("unvalidated FK produces only VALIDATE op", async () => {
    // Create tables and add FK with NOT VALID
    await ctx.client.query(`CREATE TABLE authors (id serial PRIMARY KEY)`);
    await ctx.client.query(`CREATE TABLE books (id serial PRIMARY KEY, author_id integer)`);
    await ctx.client.query(
      `ALTER TABLE books ADD CONSTRAINT fk_books_author_id_authors FOREIGN KEY (author_id) REFERENCES authors(id) NOT VALID`,
    );

    const desired: TableSchema[] = [
      {
        table: "authors",
        columns: [{ name: "id", type: "serial", primary_key: true }],
      },
      {
        table: "books",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          {
            name: "author_id",
            type: "integer",
            references: { table: "authors", column: "id" },
          },
        ],
      },
    ];

    const plan = await buildPlan(ctx.client, desired, "public");

    // Should NOT add a new FK (it already exists)
    const addFkOps = plan.foreignKeyOps.filter((o) => o.type === "add_foreign_key_not_valid");
    expect(addFkOps).toHaveLength(0);

    // Should have only a VALIDATE op
    const validateOps = plan.validateOps.filter(
      (o) => o.type === "validate_constraint" && o.sql.includes("fk_books_author_id_authors"),
    );
    expect(validateOps).toHaveLength(1);
  });

  it("SET NOT NULL uses 4-step pattern, all non-destructive", async () => {
    await ctx.client.query(`CREATE TABLE items (id serial PRIMARY KEY, name text)`);

    const desired: TableSchema[] = [
      {
        table: "items",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "name", type: "text" }, // NOT NULL by convention
        ],
      },
    ];

    const plan = await buildPlan(ctx.client, desired, "public");

    // Should NOT be blocked (safe NOT NULL uses 4-step)
    const blockedNotNull = plan.blocked.find((o) => o.sql.includes("SET NOT NULL"));
    expect(blockedNotNull).toBeUndefined();

    // Step 1: ADD CHECK ... NOT VALID in structure phase
    const addCheck = plan.structureOps.find(
      (o) => o.type === "add_check_not_valid" && o.sql.includes("chk_items_name_nn"),
    );
    expect(addCheck).toBeDefined();
    expect(addCheck!.destructive).toBe(false);

    // Steps 2-4 in validate phase
    const validateGroup = plan.validateOps.filter((o) => o.group === "safe_not_null_items_name");
    expect(validateGroup).toHaveLength(3);
    // All non-destructive
    expect(validateGroup.every((o) => !o.destructive)).toBe(true);
  });

  it("CHECK on existing table uses NOT VALID + VALIDATE", async () => {
    await ctx.client.query(`CREATE TABLE products (id serial PRIMARY KEY, price integer NOT NULL)`);

    const desired: TableSchema[] = [
      {
        table: "products",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "price", type: "integer" },
        ],
        checks: [{ name: "chk_positive_price", expression: "price > 0" }],
      },
    ];

    const plan = await buildPlan(ctx.client, desired, "public");

    // Should have NOT VALID check in structure
    const addCheck = plan.structureOps.find(
      (o) => o.type === "add_check_not_valid" && o.sql.includes("chk_positive_price"),
    );
    expect(addCheck).toBeDefined();
    expect(addCheck!.sql).toContain("NOT VALID");

    // Should have VALIDATE in validate phase
    const validate = plan.validateOps.find(
      (o) => o.type === "validate_constraint" && o.sql.includes("chk_positive_price"),
    );
    expect(validate).toBeDefined();
  });

  it("UNIQUE on existing column uses concurrent index + constraint using index", async () => {
    await ctx.client.query(`CREATE TABLE users (id serial PRIMARY KEY, email varchar(255) NOT NULL)`);

    const desired: TableSchema[] = [
      {
        table: "users",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "email", type: "varchar(255)", unique: true },
        ],
      },
    ];

    const plan = await buildPlan(ctx.client, desired, "public");

    // Should have a concurrent unique index in structure phase
    const idxOp = plan.structureOps.find((o) => o.type === "add_unique_index");
    expect(idxOp).toBeDefined();
    expect(idxOp!.sql).toContain("CREATE UNIQUE INDEX CONCURRENTLY");
    expect(idxOp!.sql).toContain("idx_users_email_unique");

    // Should have ADD CONSTRAINT USING INDEX in validate phase
    const constraintOp = plan.validateOps.find((o) => o.sql.includes("UNIQUE USING INDEX"));
    expect(constraintOp).toBeDefined();
    expect(constraintOp!.sql).toContain("uq_users_email");
  });

  it("plans multiple tables with correct FK ordering", async () => {
    const desired: TableSchema[] = [
      {
        table: "tags",
        columns: [{ name: "id", type: "serial", primary_key: true }],
      },
      {
        table: "posts",
        columns: [{ name: "id", type: "serial", primary_key: true }],
      },
      {
        table: "post_tags",
        columns: [
          {
            name: "post_id",
            type: "integer",
            references: { table: "posts", column: "id", on_delete: "CASCADE" },
          },
          {
            name: "tag_id",
            type: "integer",
            references: { table: "tags", column: "id", on_delete: "CASCADE" },
          },
        ],
        primary_key: ["post_id", "tag_id"],
      },
    ];

    const plan = await buildPlan(ctx.client, desired, "public");

    // All CREATE TABLE ops should come before any FK ops
    const allOps = plan.operations;
    const lastStructureIdx = allOps.findLastIndex((o) => o.phase === "structure");
    const firstFkIdx = allOps.findIndex((o) => o.phase === "foreign_key");

    if (firstFkIdx !== -1) {
      expect(lastStructureIdx).toBeLessThan(firstFkIdx);
    }

    // 2 FKs with NOT VALID
    expect(plan.foreignKeyOps).toHaveLength(2);
    // 2 VALIDATE CONSTRAINT ops
    expect(plan.validateOps).toHaveLength(2);
  });

  it("orders CREATE TABLE before cross-table policies in structureOps", async () => {
    // Table B has a policy that references table A via subquery.
    // If B is listed before A, the policy must still come after A's CREATE TABLE.
    const desired: TableSchema[] = [
      {
        table: "batch_ingredients",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "plant_id", type: "integer" },
        ],
        rls: true,
        policies: [
          {
            name: "session_authorization",
            for: "ALL",
            to: ["PUBLIC"],
            using: "(EXISTS (SELECT 1 FROM plants p WHERE p.id = batch_ingredients.plant_id))",
          },
        ],
      },
      {
        table: "plants",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "name", type: "text" },
        ],
      },
    ];

    const plan = await buildPlan(ctx.client, desired, "public");

    // Every create_table op must appear before every create_policy op
    const lastCreateTableIdx = plan.structureOps.findLastIndex((o) => o.type === "create_table");
    const firstPolicyIdx = plan.structureOps.findIndex((o) => o.type === "create_policy");

    expect(firstPolicyIdx).toBeGreaterThan(lastCreateTableIdx);
  });
});
