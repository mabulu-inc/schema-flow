// test/sql.test.ts
// Tests for SQL file generation

import { describe, it, expect } from "vitest";
import { formatMigrationSql } from "../src/sql/index.js";
import type { MigrationPlan, Operation } from "../src/planner/index.js";
import type { FunctionSchema } from "../src/schema/types.js";

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

function makePlan(ops: Partial<Operation>[]): MigrationPlan {
  const fullOps = ops.map(makeOp);
  const structureOps = fullOps.filter((o) => o.phase === "structure");
  const foreignKeyOps = fullOps.filter((o) => o.phase === "foreign_key");
  const validateOps = fullOps.filter((o) => o.phase === "validate");

  return {
    operations: fullOps,
    structureOps,
    foreignKeyOps,
    validateOps,
    blocked: [],
    summary: {
      tablesToCreate: ["users"],
      tablesToAlter: [],
      foreignKeysToAdd: foreignKeyOps.length,
      validateOpsCount: validateOps.length,
      totalOperations: fullOps.length,
      destructiveCount: 0,
      blockedCount: 0,
    },
  };
}

describe("SQL file generation", () => {
  it("includes header with metadata", () => {
    const plan = makePlan([
      {
        type: "create_table",
        table: "users",
        sql: 'CREATE TABLE "public"."users" ("id" serial PRIMARY KEY);',
        description: "Create table users",
      },
    ]);

    const sql = formatMigrationSql(plan, [], {
      timestamp: "2026-03-02T15:30:00Z",
      version: "1.0.0",
      pgSchema: "public",
    });

    expect(sql).toContain("-- schema-flow migration");
    expect(sql).toContain("2026-03-02T15:30:00Z");
    expect(sql).toContain("v1.0.0");
    expect(sql).toContain("public");
  });

  it("wraps structure ops in BEGIN/COMMIT", () => {
    const plan = makePlan([
      {
        type: "create_table",
        table: "users",
        sql: 'CREATE TABLE "public"."users" ("id" serial PRIMARY KEY);',
        description: "Create table users",
      },
    ]);

    const sql = formatMigrationSql(plan, []);
    expect(sql).toContain("BEGIN;");
    expect(sql).toContain("CREATE TABLE");
    expect(sql).toContain("COMMIT;");
  });

  it("puts index ops outside transaction with CONCURRENTLY comment", () => {
    const plan = makePlan([
      {
        type: "add_index",
        table: "users",
        sql: 'CREATE INDEX CONCURRENTLY IF NOT EXISTS "idx_users_email" ON "public"."users" ("email");',
        description: "Create index on users.email",
        phase: "structure",
      },
    ]);

    const sql = formatMigrationSql(plan, []);
    expect(sql).toContain("-- Indexes (outside txn");
    expect(sql).toContain("CREATE INDEX CONCURRENTLY");
    // Should NOT be wrapped in BEGIN/COMMIT
    const beginIdx = sql.indexOf("BEGIN;");
    const indexIdx = sql.indexOf("CREATE INDEX CONCURRENTLY");
    // If there's a BEGIN, the index should not be between BEGIN and COMMIT
    if (beginIdx !== -1) {
      const commitIdx = sql.indexOf("COMMIT;", beginIdx);
      expect(indexIdx > commitIdx || indexIdx < beginIdx).toBe(true);
    }
  });

  it("wraps FK ops in their own transaction", () => {
    const plan = makePlan([
      {
        type: "add_foreign_key_not_valid",
        table: "books",
        sql: 'ALTER TABLE "public"."books" ADD CONSTRAINT "fk_books_author" FOREIGN KEY ("author_id") REFERENCES "public"."authors" ("id") NOT VALID;',
        description: "Add FK",
        phase: "foreign_key",
      },
    ]);

    const sql = formatMigrationSql(plan, []);
    expect(sql).toContain("-- Foreign keys");
    expect(sql).toContain("BEGIN;");
    expect(sql).toContain("FOREIGN KEY");
    expect(sql).toContain("COMMIT;");
  });

  it("includes validate ops outside transaction", () => {
    const plan = makePlan([
      {
        type: "validate_constraint",
        table: "books",
        sql: 'ALTER TABLE "public"."books" VALIDATE CONSTRAINT "fk_books_author";',
        description: "Validate FK",
        phase: "validate",
      },
    ]);

    const sql = formatMigrationSql(plan, []);
    expect(sql).toContain("-- Validate (outside txn)");
    expect(sql).toContain("VALIDATE CONSTRAINT");
  });

  it("includes functions before structure ops", () => {
    const plan = makePlan([
      {
        type: "create_table",
        table: "events",
        sql: 'CREATE TABLE "public"."events" ("id" serial PRIMARY KEY);',
        description: "Create table events",
      },
    ]);

    const functions: FunctionSchema[] = [
      {
        name: "update_timestamp",
        language: "plpgsql",
        returns: "trigger",
        body: "BEGIN\n  NEW.updated_at = now();\n  RETURN NEW;\nEND;",
        replace: true,
      },
    ];

    const sql = formatMigrationSql(plan, functions);
    const funcIdx = sql.indexOf("CREATE OR REPLACE FUNCTION update_timestamp");
    const tableIdx = sql.indexOf("CREATE TABLE");
    expect(funcIdx).toBeLessThan(tableIdx);
    expect(sql).toContain("-- Functions");
  });

  it("handles empty plan", () => {
    const plan: MigrationPlan = {
      operations: [],
      structureOps: [],
      foreignKeyOps: [],
      validateOps: [],
      blocked: [],
      summary: {
        tablesToCreate: [],
        tablesToAlter: [],
        foreignKeysToAdd: 0,
        validateOpsCount: 0,
        totalOperations: 0,
        destructiveCount: 0,
        blockedCount: 0,
      },
    };

    const sql = formatMigrationSql(plan, []);
    expect(sql).toContain("-- schema-flow migration");
    // Should not have BEGIN/COMMIT
    expect(sql).not.toContain("BEGIN;");
  });
});
