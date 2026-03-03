// src/lint/rules.ts
// Built-in lint rule implementations

import type { LintRule, LintContext, LintFinding } from "./index.js";

/** Detect SET NOT NULL without the safe 4-step group pattern */
export const setNotNullDirect: LintRule = {
  id: "lock/set-not-null-direct",
  name: "Direct SET NOT NULL",
  severity: "warning",
  check(ctx: LintContext): LintFinding[] {
    const findings: LintFinding[] = [];
    for (const op of ctx.plan.operations) {
      if (op.type === "alter_column" && op.sql.includes("SET NOT NULL") && !op.group?.startsWith("safe_not_null_")) {
        findings.push({
          severity: "warning",
          rule: this.id,
          table: op.table,
          message: `SET NOT NULL without safe 4-step pattern. Consider using the safe NOT NULL approach to avoid table locks.`,
        });
      }
    }
    return findings;
  },
};

/** Info: ADD COLUMN with DEFAULT (safe since PG11, but worth noting) */
export const addColumnWithDefault: LintRule = {
  id: "lock/add-column-with-default",
  name: "ADD COLUMN with DEFAULT",
  severity: "info",
  check(ctx: LintContext): LintFinding[] {
    const findings: LintFinding[] = [];
    for (const op of ctx.plan.operations) {
      if (op.type === "add_column" && op.sql.includes("DEFAULT")) {
        findings.push({
          severity: "info",
          rule: this.id,
          table: op.table,
          message: `ADD COLUMN with DEFAULT is safe on PG 11+ (no table rewrite). Verify your PG version if concerned.`,
        });
      }
    }
    return findings;
  },
};

/** Warning: any DROP COLUMN */
export const dropColumn: LintRule = {
  id: "data/drop-column",
  name: "Drop Column",
  severity: "warning",
  check(ctx: LintContext): LintFinding[] {
    const findings: LintFinding[] = [];
    const ops = [...ctx.plan.operations, ...ctx.plan.blocked];
    for (const op of ops) {
      if (op.type === "drop_column") {
        const colMatch = op.sql.match(/DROP COLUMN "([^"]+)"/);
        findings.push({
          severity: "warning",
          rule: this.id,
          table: op.table,
          column: colMatch?.[1],
          message: `DROP COLUMN detected. This will permanently delete data. Ensure you have a backup or migration plan.`,
        });
      }
    }
    return findings;
  },
};

/** Warning: any DROP TABLE */
export const dropTable: LintRule = {
  id: "data/drop-table",
  name: "Drop Table",
  severity: "warning",
  check(ctx: LintContext): LintFinding[] {
    const findings: LintFinding[] = [];
    const ops = [...ctx.plan.operations, ...ctx.plan.blocked];
    for (const op of ops) {
      if (op.type === "drop_table") {
        findings.push({
          severity: "warning",
          rule: this.id,
          table: op.table,
          message: `DROP TABLE detected. This will permanently delete the table and all its data.`,
        });
      }
    }
    return findings;
  },
};

/** Warning: destructive type changes (narrowing) */
export const typeNarrowing: LintRule = {
  id: "data/type-narrowing",
  name: "Type Narrowing",
  severity: "warning",
  check(ctx: LintContext): LintFinding[] {
    const findings: LintFinding[] = [];
    const ops = [...ctx.plan.operations, ...ctx.plan.blocked];
    for (const op of ops) {
      if (op.type === "alter_column" && op.destructive && op.sql.includes("TYPE")) {
        findings.push({
          severity: "warning",
          rule: this.id,
          table: op.table,
          message: `Destructive type change: ${op.description}. This may cause data loss or errors.`,
        });
      }
    }
    return findings;
  },
};

/** Warning: FK columns without matching index */
export const missingFkIndex: LintRule = {
  id: "perf/missing-fk-index",
  name: "Missing FK Index",
  severity: "warning",
  check(ctx: LintContext): LintFinding[] {
    const findings: LintFinding[] = [];
    for (const schema of ctx.schemas) {
      const indexedCols = new Set<string>();
      if (schema.indexes) {
        for (const idx of schema.indexes) {
          // First column of a multi-column index counts
          if (idx.columns.length > 0) {
            indexedCols.add(idx.columns[0]);
          }
        }
      }

      for (const col of schema.columns) {
        if (col.references && !col.primary_key && !col.unique && !indexedCols.has(col.name)) {
          findings.push({
            severity: "warning",
            rule: this.id,
            table: schema.table,
            column: col.name,
            message: `FK column "${col.name}" on "${schema.table}" has no matching index. This can cause slow deletes and joins.`,
          });
        }
      }
    }
    return findings;
  },
};

/** Info: detect possible rename (drop + add same-type columns) */
export const renameDetection: LintRule = {
  id: "compat/rename-detection",
  name: "Rename Detection",
  severity: "info",
  check(ctx: LintContext): LintFinding[] {
    const findings: LintFinding[] = [];
    const ops = [...ctx.plan.operations, ...ctx.plan.blocked];

    // Group drops and adds by table
    const dropsByTable = new Map<string, { col: string; type: string }[]>();
    const addsByTable = new Map<string, { col: string; type: string }[]>();

    for (const op of ops) {
      if (op.type === "drop_column" && op.table) {
        const colMatch = op.sql.match(/DROP COLUMN "([^"]+)"/);
        if (colMatch) {
          if (!dropsByTable.has(op.table)) dropsByTable.set(op.table, []);
          // We don't have the type from the DROP SQL, so we check schemas
          const schema = ctx.schemas.find((s) => s.table === op.table);
          const colDef = schema?.columns.find((c) => c.name === colMatch[1]);
          dropsByTable.get(op.table)!.push({ col: colMatch[1], type: colDef?.type || "unknown" });
        }
      }
      if (op.type === "add_column" && op.table) {
        const colMatch = op.sql.match(/ADD COLUMN "([^"]+)" (\S+)/);
        if (colMatch) {
          if (!addsByTable.has(op.table)) addsByTable.set(op.table, []);
          addsByTable.get(op.table)!.push({ col: colMatch[1], type: colMatch[2] });
        }
      }
    }

    for (const [table, drops] of dropsByTable) {
      const adds = addsByTable.get(table) || [];
      for (const drop of drops) {
        for (const add of adds) {
          if (drop.type.toLowerCase() === add.type.toLowerCase().replace(/;$/, "")) {
            findings.push({
              severity: "info",
              rule: this.id,
              table,
              message: `Possible rename detected: "${drop.col}" → "${add.col}" (both ${drop.type}). Consider using a pre-migration script with ALTER TABLE RENAME COLUMN instead.`,
            });
          }
        }
      }
    }

    return findings;
  },
};

/** Warning: any column type change */
export const typeChange: LintRule = {
  id: "compat/type-change",
  name: "Type Change",
  severity: "warning",
  check(ctx: LintContext): LintFinding[] {
    const findings: LintFinding[] = [];
    const ops = [...ctx.plan.operations, ...ctx.plan.blocked];
    for (const op of ops) {
      if (op.type === "alter_column" && op.sql.includes("TYPE") && !op.destructive) {
        findings.push({
          severity: "warning",
          rule: this.id,
          table: op.table,
          message: `Column type change: ${op.description}. Verify that existing data is compatible.`,
        });
      }
    }
    return findings;
  },
};

/** All built-in rules */
export const builtinRules: LintRule[] = [
  setNotNullDirect,
  addColumnWithDefault,
  dropColumn,
  dropTable,
  typeNarrowing,
  missingFkIndex,
  renameDetection,
  typeChange,
];
