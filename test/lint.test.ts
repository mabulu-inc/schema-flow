// test/lint.test.ts
// Tests for migration linting

import { describe, it, expect } from "vitest";
import { lintPlan, formatLintFindings, formatLintFindingsJson, type LintContext } from "../src/lint/index.js";
import type { MigrationPlan, Operation } from "../src/planner/index.js";
import type { TableSchema } from "../src/schema/types.js";

function makePlan(ops: Partial<Operation>[], blocked: Partial<Operation>[] = []): MigrationPlan {
  const fullOps = ops.map((o) => ({
    type: o.type || "alter_column",
    table: o.table,
    sql: o.sql || "",
    description: o.description || "",
    phase: o.phase || "structure",
    destructive: o.destructive ?? false,
    group: o.group,
    ...o,
  })) as Operation[];

  const fullBlocked = blocked.map((o) => ({
    type: o.type || "alter_column",
    table: o.table,
    sql: o.sql || "",
    description: o.description || "",
    phase: o.phase || "structure",
    destructive: o.destructive ?? true,
    group: o.group,
    ...o,
  })) as Operation[];

  return {
    operations: fullOps,
    structureOps: fullOps.filter((o) => o.phase === "structure"),
    foreignKeyOps: fullOps.filter((o) => o.phase === "foreign_key"),
    validateOps: fullOps.filter((o) => o.phase === "validate"),
    blocked: fullBlocked,
    summary: {
      tablesToCreate: [],
      tablesToAlter: [],
      foreignKeysToAdd: 0,
      validateOpsCount: 0,
      totalOperations: fullOps.length,
      destructiveCount: fullOps.filter((o) => o.destructive).length,
      blockedCount: fullBlocked.length,
    },
  };
}

function makeCtx(plan: MigrationPlan, schemas: TableSchema[] = []): LintContext {
  return { plan, schemas, pgSchema: "public" };
}

describe("lint", () => {
  it("returns no findings for clean plan", () => {
    const plan = makePlan([
      {
        type: "create_table",
        table: "users",
        sql: 'CREATE TABLE "public"."users" ("id" serial PRIMARY KEY);',
        description: "Create table users",
      },
    ]);

    const findings = lintPlan(makeCtx(plan));
    expect(findings).toHaveLength(0);
  });

  it("detects DROP COLUMN", () => {
    const plan = makePlan(
      [],
      [
        {
          type: "drop_column",
          table: "users",
          sql: 'ALTER TABLE "public"."users" DROP COLUMN "name";',
          description: "Drop column users.name",
          destructive: true,
        },
      ],
    );

    const findings = lintPlan(makeCtx(plan));
    const dropFinding = findings.find((f) => f.rule === "data/drop-column");
    expect(dropFinding).toBeDefined();
    expect(dropFinding!.severity).toBe("warning");
    expect(dropFinding!.table).toBe("users");
  });

  it("detects missing FK index", () => {
    const schemas: TableSchema[] = [
      {
        table: "orders",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "user_id", type: "integer", references: { table: "users", column: "id" } },
        ],
      },
    ];

    const plan = makePlan([]);
    const findings = lintPlan(makeCtx(plan, schemas));
    const fkFinding = findings.find((f) => f.rule === "perf/missing-fk-index");
    expect(fkFinding).toBeDefined();
    expect(fkFinding!.column).toBe("user_id");
  });

  it("does not warn about FK index when index exists", () => {
    const schemas: TableSchema[] = [
      {
        table: "orders",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "user_id", type: "integer", references: { table: "users", column: "id" } },
        ],
        indexes: [{ columns: ["user_id"] }],
      },
    ];

    const plan = makePlan([]);
    const findings = lintPlan(makeCtx(plan, schemas));
    const fkFinding = findings.find((f) => f.rule === "perf/missing-fk-index");
    expect(fkFinding).toBeUndefined();
  });

  it("detects destructive type narrowing", () => {
    const plan = makePlan(
      [],
      [
        {
          type: "alter_column",
          table: "products",
          sql: 'ALTER TABLE "public"."products" ALTER COLUMN "price" TYPE integer USING "price"::integer;',
          description: "Change type of products.price: bigint → integer",
          destructive: true,
        },
      ],
    );

    const findings = lintPlan(makeCtx(plan));
    const typeFinding = findings.find((f) => f.rule === "data/type-narrowing");
    expect(typeFinding).toBeDefined();
    expect(typeFinding!.severity).toBe("warning");
  });

  it("detects ADD COLUMN with DEFAULT as info", () => {
    const plan = makePlan([
      {
        type: "add_column",
        table: "users",
        sql: 'ALTER TABLE "public"."users" ADD COLUMN "active" boolean DEFAULT true;',
        description: "Add column users.active",
      },
    ]);

    const findings = lintPlan(makeCtx(plan));
    const defaultFinding = findings.find((f) => f.rule === "lock/add-column-with-default");
    expect(defaultFinding).toBeDefined();
    expect(defaultFinding!.severity).toBe("info");
  });

  it("detects possible rename (drop + add same type)", () => {
    const schemas: TableSchema[] = [
      {
        table: "users",
        columns: [{ name: "id", type: "serial", primary_key: true }],
      },
    ];

    const plan = makePlan(
      [
        {
          type: "add_column",
          table: "users",
          sql: 'ALTER TABLE "public"."users" ADD COLUMN "full_name" text;',
          description: "Add column users.full_name",
        },
      ],
      [
        {
          type: "drop_column",
          table: "users",
          sql: 'ALTER TABLE "public"."users" DROP COLUMN "name";',
          description: "Drop column users.name",
          destructive: true,
        },
      ],
    );

    // Adjust schema to have original column for type lookup
    schemas[0].columns.push({ name: "name", type: "text" });

    const findings = lintPlan(makeCtx(plan, schemas));
    const renameFinding = findings.find((f) => f.rule === "compat/rename-detection");
    expect(renameFinding).toBeDefined();
    expect(renameFinding!.severity).toBe("info");
    expect(renameFinding!.message).toContain("name");
    expect(renameFinding!.message).toContain("full_name");
  });

  it("detects safe type change as warning", () => {
    const plan = makePlan([
      {
        type: "alter_column",
        table: "metrics",
        sql: 'ALTER TABLE "public"."metrics" ALTER COLUMN "count" TYPE bigint USING "count"::bigint;',
        description: "Change type of metrics.count: integer → bigint",
        destructive: false,
      },
    ]);

    const findings = lintPlan(makeCtx(plan));
    const typeFinding = findings.find((f) => f.rule === "compat/type-change");
    expect(typeFinding).toBeDefined();
    expect(typeFinding!.severity).toBe("warning");
  });

  it("sorts findings by severity (error > warning > info)", () => {
    const schemas: TableSchema[] = [
      {
        table: "orders",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "user_id", type: "integer", references: { table: "users", column: "id" } },
        ],
      },
    ];

    const plan = makePlan(
      [
        {
          type: "add_column",
          table: "orders",
          sql: 'ALTER TABLE "public"."orders" ADD COLUMN "active" boolean DEFAULT true;',
          description: "Add column",
        },
      ],
      [
        {
          type: "drop_column",
          table: "orders",
          sql: 'ALTER TABLE "public"."orders" DROP COLUMN "old";',
          description: "Drop column",
          destructive: true,
        },
      ],
    );

    const findings = lintPlan(makeCtx(plan, schemas));
    expect(findings.length).toBeGreaterThan(0);
    // Verify ordering
    for (let i = 1; i < findings.length; i++) {
      const order: Record<string, number> = { error: 0, warning: 1, info: 2 };
      expect(order[findings[i].severity]).toBeGreaterThanOrEqual(order[findings[i - 1].severity]);
    }
  });

  describe("formatLintFindings", () => {
    it("returns no-issues message for empty findings", () => {
      const out = formatLintFindings([]);
      expect(out).toBe("No lint issues found.");
    });

    it("formats findings with severity symbols and scope", () => {
      const out = formatLintFindings([
        { severity: "warning", rule: "data/drop-column", table: "users", column: "name", message: "Dropping column" },
        { severity: "info", rule: "lock/add-column-with-default", table: "users", message: "Default present" },
      ]);
      expect(out).toContain("2 lint issue(s) found:");
      expect(out).toContain("W data/drop-column [users.name]: Dropping column");
      expect(out).toContain("I lock/add-column-with-default [users]: Default present");
      expect(out).toContain("0 error(s), 1 warning(s), 1 info(s)");
    });
  });

  describe("formatLintFindingsJson", () => {
    it("returns valid JSON with findings and count", () => {
      const findings = [
        { severity: "warning" as const, rule: "data/drop-column", table: "users", message: "test" },
      ];
      const json = formatLintFindingsJson(findings);
      const parsed = JSON.parse(json);
      expect(parsed.count).toBe(1);
      expect(parsed.findings).toHaveLength(1);
      expect(parsed.findings[0].rule).toBe("data/drop-column");
    });
  });
});
