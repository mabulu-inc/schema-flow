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

    // FK phase should have the constraint
    expect(plan.foreignKeyOps.length).toBeGreaterThan(0);
    expect(plan.foreignKeyOps[0].sql).toContain("FOREIGN KEY");
    expect(plan.foreignKeyOps[0].sql).toContain("CASCADE");
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

  it("blocks SET NOT NULL as destructive (can fail with existing NULLs)", async () => {
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

    // SET NOT NULL is destructive
    const setNotNullBlocked = plan.blocked.find((o) => o.sql.includes("SET NOT NULL"));
    expect(setNotNullBlocked).toBeDefined();
    expect(setNotNullBlocked!.destructive).toBe(true);
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

    expect(plan.foreignKeyOps).toHaveLength(2);
  });
});
