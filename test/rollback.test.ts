// test/rollback.test.ts
// Tests for down migrations / rollback

import { describe, it, expect } from "vitest";
import { computeRollback, type RollbackResult } from "../src/rollback/index.js";
import type { Operation } from "../src/planner/index.js";
import type { MigrationSnapshot } from "../src/rollback/snapshot.js";

const emptySnapshot: MigrationSnapshot = { tables: {}, capturedAt: "" };

function makeOp(overrides: Partial<Operation>): Operation {
  return {
    type: overrides.type || "create_table",
    table: overrides.table,
    sql: overrides.sql || "",
    description: overrides.description || "",
    phase: overrides.phase || "structure",
    destructive: overrides.destructive ?? false,
    ...overrides,
  } as Operation;
}

describe("rollback", () => {
  it("reverses CREATE TABLE to DROP TABLE", () => {
    const ops: Operation[] = [
      makeOp({
        type: "create_table",
        table: "users",
        sql: 'CREATE TABLE "public"."users" ("id" serial PRIMARY KEY);',
        description: "Create table users",
      }),
    ];

    const result = computeRollback(ops, emptySnapshot, "public");
    expect(result.operations).toHaveLength(1);
    expect(result.operations[0].sql).toContain("DROP TABLE");
    expect(result.operations[0].destructive).toBe(true);
  });

  it("reverses ADD COLUMN to DROP COLUMN", () => {
    const ops: Operation[] = [
      makeOp({
        type: "add_column",
        table: "users",
        sql: 'ALTER TABLE "public"."users" ADD COLUMN "email" varchar(255);',
        description: "Add column users.email",
      }),
    ];

    const result = computeRollback(ops, emptySnapshot, "public");
    expect(result.operations).toHaveLength(1);
    expect(result.operations[0].sql).toContain("DROP COLUMN");
    expect(result.operations[0].sql).toContain("email");
  });

  it("reverses SET NOT NULL to DROP NOT NULL", () => {
    const ops: Operation[] = [
      makeOp({
        type: "alter_column",
        table: "users",
        sql: 'ALTER TABLE "public"."users" ALTER COLUMN "name" SET NOT NULL;',
        description: "Set NOT NULL",
      }),
    ];

    const result = computeRollback(ops, emptySnapshot, "public");
    expect(result.operations).toHaveLength(1);
    expect(result.operations[0].sql).toContain("DROP NOT NULL");
    expect(result.operations[0].safe).toBe(true);
  });

  it("reverses ADD INDEX to DROP INDEX CONCURRENTLY", () => {
    const ops: Operation[] = [
      makeOp({
        type: "add_index",
        table: "users",
        sql: 'CREATE INDEX CONCURRENTLY IF NOT EXISTS "idx_users_email" ON "public"."users" ("email");',
        description: "Create index",
      }),
    ];

    const result = computeRollback(ops, emptySnapshot, "public");
    expect(result.operations).toHaveLength(1);
    expect(result.operations[0].sql).toContain("DROP INDEX CONCURRENTLY");
    expect(result.operations[0].sql).toContain("idx_users_email");
  });

  it("reverses ADD FK to DROP CONSTRAINT", () => {
    const ops: Operation[] = [
      makeOp({
        type: "add_foreign_key_not_valid",
        table: "books",
        sql: 'ALTER TABLE "public"."books" ADD CONSTRAINT "fk_books_author" FOREIGN KEY ("author_id") REFERENCES "public"."authors" ("id") NOT VALID;',
        description: "Add FK",
        phase: "foreign_key",
      }),
    ];

    const result = computeRollback(ops, emptySnapshot, "public");
    expect(result.operations).toHaveLength(1);
    expect(result.operations[0].sql).toContain("DROP CONSTRAINT");
    expect(result.operations[0].sql).toContain("fk_books_author");
  });

  it("skips VALIDATE CONSTRAINT (can't un-validate)", () => {
    const ops: Operation[] = [
      makeOp({
        type: "validate_constraint",
        table: "books",
        sql: 'ALTER TABLE "public"."books" VALIDATE CONSTRAINT "fk_books_author";',
        description: "Validate FK",
        phase: "validate",
      }),
    ];

    const result = computeRollback(ops, emptySnapshot, "public");
    expect(result.operations).toHaveLength(0);
  });

  it("marks DROP operations as irreversible", () => {
    const ops: Operation[] = [
      makeOp({
        type: "drop_column",
        table: "users",
        sql: 'ALTER TABLE "public"."users" DROP COLUMN "old";',
        description: "Drop column users.old",
        destructive: true,
      }),
    ];

    const result = computeRollback(ops, emptySnapshot, "public");
    expect(result.operations).toHaveLength(1);
    expect(result.operations[0].irreversible).toBe(true);
    expect(result.hasIrreversible).toBe(true);
  });

  it("reverses type change using snapshot", () => {
    const snapshot: MigrationSnapshot = {
      tables: {
        products: {
          columns: {
            price: { type: "int4", nullable: false },
          },
        },
      },
      capturedAt: "",
    };

    const ops: Operation[] = [
      makeOp({
        type: "alter_column",
        table: "products",
        sql: 'ALTER TABLE "public"."products" ALTER COLUMN "price" TYPE bigint USING "price"::bigint;',
        description: "Change type",
      }),
    ];

    const result = computeRollback(ops, snapshot, "public");
    expect(result.operations).toHaveLength(1);
    expect(result.operations[0].sql).toContain("TYPE int4");
  });

  it("reverses CREATE TRIGGER to DROP TRIGGER", () => {
    const ops: Operation[] = [
      makeOp({
        type: "create_trigger",
        table: "events",
        sql: 'CREATE TRIGGER "set_updated" BEFORE UPDATE ON "public"."events" FOR EACH ROW EXECUTE FUNCTION update_timestamp();',
        description: "Create trigger",
      }),
    ];

    const result = computeRollback(ops, emptySnapshot, "public");
    expect(result.operations).toHaveLength(1);
    expect(result.operations[0].sql).toContain("DROP TRIGGER");
    expect(result.operations[0].sql).toContain("set_updated");
  });

  it("reverses enable_rls to disable", () => {
    const ops: Operation[] = [
      makeOp({
        type: "enable_rls",
        table: "orders",
        sql: 'ALTER TABLE "public"."orders" ENABLE ROW LEVEL SECURITY;',
        description: "Enable RLS",
      }),
    ];

    const result = computeRollback(ops, emptySnapshot, "public");
    expect(result.operations).toHaveLength(1);
    expect(result.operations[0].sql).toContain("DISABLE ROW LEVEL SECURITY");
  });

  it("reverses DROP NOT NULL to SET NOT NULL", () => {
    const ops: Operation[] = [
      makeOp({
        type: "alter_column",
        table: "users",
        sql: 'ALTER TABLE "public"."users" ALTER COLUMN "name" DROP NOT NULL;',
        description: "Drop NOT NULL",
      }),
    ];

    const result = computeRollback(ops, emptySnapshot, "public");
    expect(result.operations).toHaveLength(1);
    expect(result.operations[0].sql).toContain("SET NOT NULL");
    expect(result.operations[0].safe).toBe(false);
  });

  it("reverses SET DEFAULT to DROP DEFAULT when no snapshot", () => {
    const ops: Operation[] = [
      makeOp({
        type: "alter_column",
        table: "users",
        sql: 'ALTER TABLE "public"."users" ALTER COLUMN "status" SET DEFAULT \'active\';',
        description: "Set default",
      }),
    ];

    const result = computeRollback(ops, emptySnapshot, "public");
    expect(result.operations).toHaveLength(1);
    expect(result.operations[0].sql).toContain("DROP DEFAULT");
  });

  it("reverses SET DEFAULT to original default using snapshot", () => {
    const snapshot: MigrationSnapshot = {
      tables: {
        users: { columns: { status: { type: "text", nullable: true, default: "'pending'" } } },
      },
      capturedAt: "",
    };

    const ops: Operation[] = [
      makeOp({
        type: "alter_column",
        table: "users",
        sql: 'ALTER TABLE "public"."users" ALTER COLUMN "status" SET DEFAULT \'active\';',
        description: "Set default",
      }),
    ];

    const result = computeRollback(ops, snapshot, "public");
    expect(result.operations).toHaveLength(1);
    expect(result.operations[0].sql).toContain("SET DEFAULT 'pending'");
  });

  it("reverses DROP DEFAULT to restore original default from snapshot", () => {
    const snapshot: MigrationSnapshot = {
      tables: {
        users: { columns: { status: { type: "text", nullable: true, default: "'active'" } } },
      },
      capturedAt: "",
    };

    const ops: Operation[] = [
      makeOp({
        type: "alter_column",
        table: "users",
        sql: 'ALTER TABLE "public"."users" ALTER COLUMN "status" DROP DEFAULT;',
        description: "Drop default",
      }),
    ];

    const result = computeRollback(ops, snapshot, "public");
    expect(result.operations).toHaveLength(1);
    expect(result.operations[0].sql).toContain("SET DEFAULT 'active'");
  });

  it("reverses ADD CHECK to DROP CONSTRAINT", () => {
    const ops: Operation[] = [
      makeOp({
        type: "add_check",
        table: "products",
        sql: 'ALTER TABLE "public"."products" ADD CONSTRAINT "chk_positive" CHECK (price > 0);',
        description: "Add check",
      }),
    ];

    const result = computeRollback(ops, emptySnapshot, "public");
    expect(result.operations).toHaveLength(1);
    expect(result.operations[0].sql).toContain("DROP CONSTRAINT");
    expect(result.operations[0].sql).toContain("chk_positive");
  });

  it("reverses FORCE RLS to NO FORCE", () => {
    const ops: Operation[] = [
      makeOp({
        type: "enable_rls",
        table: "orders",
        sql: 'ALTER TABLE "public"."orders" FORCE ROW LEVEL SECURITY;',
        description: "Force RLS",
      }),
    ];

    const result = computeRollback(ops, emptySnapshot, "public");
    expect(result.operations).toHaveLength(1);
    expect(result.operations[0].sql).toContain("NO FORCE ROW LEVEL SECURITY");
  });

  it("reverses CREATE POLICY to DROP POLICY", () => {
    const ops: Operation[] = [
      makeOp({
        type: "create_policy",
        table: "orders",
        sql: 'CREATE POLICY "users_see_own" ON "public"."orders" FOR SELECT USING (user_id = current_user_id());',
        description: "Create policy",
      }),
    ];

    const result = computeRollback(ops, emptySnapshot, "public");
    expect(result.operations).toHaveLength(1);
    expect(result.operations[0].sql).toContain("DROP POLICY");
    expect(result.operations[0].sql).toContain("users_see_own");
  });

  it("returns null for unknown operation types", () => {
    const ops: Operation[] = [
      makeOp({
        type: "backfill_column" as any,
        table: "users",
        sql: "-- backfill",
        description: "Backfill",
      }),
    ];

    const result = computeRollback(ops, emptySnapshot, "public");
    expect(result.operations).toHaveLength(0);
  });

  it("reverses operations in reverse order", () => {
    const ops: Operation[] = [
      makeOp({
        type: "create_table",
        table: "users",
        sql: 'CREATE TABLE "public"."users" ("id" serial PRIMARY KEY);',
        description: "Create table users",
      }),
      makeOp({
        type: "add_column",
        table: "users",
        sql: 'ALTER TABLE "public"."users" ADD COLUMN "email" varchar(255);',
        description: "Add column email",
      }),
    ];

    const result = computeRollback(ops, emptySnapshot, "public");
    expect(result.operations).toHaveLength(2);
    // Should drop column first, then drop table
    expect(result.operations[0].sql).toContain("DROP COLUMN");
    expect(result.operations[1].sql).toContain("DROP TABLE");
  });
});
