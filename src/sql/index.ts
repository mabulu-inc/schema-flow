// src/sql/index.ts
// SQL file generation: format migration plans as executable SQL files

import { writeFileSync, mkdirSync, existsSync } from "node:fs";
import path from "node:path";
import type { MigrationPlan } from "../planner/index.js";
import type { FunctionSchema } from "../schema/types.js";
import { utcTimestamp } from "../core/files.js";

export interface SqlFormatOptions {
  /** UTC timestamp for the header */
  timestamp?: string;
  /** schema-flow version */
  version?: string;
  /** PostgreSQL schema name */
  pgSchema?: string;
}

export interface SqlGenerateResult {
  sql: string;
  filePath: string;
  operationCount: number;
}

/** Format a migration plan (and optional functions) as a structured SQL string */
export function formatMigrationSql(
  plan: MigrationPlan,
  functions: FunctionSchema[],
  options: SqlFormatOptions = {},
): string {
  const ts = options.timestamp || new Date().toISOString();
  const version = options.version || "1.0.0";
  const pgSchema = options.pgSchema || "public";

  const tables = [...plan.summary.tablesToCreate, ...plan.summary.tablesToAlter];
  const tableList = tables.length > 0 ? tables.join(", ") : "(none)";
  const totalOps = functions.length + plan.structureOps.length + plan.foreignKeyOps.length + plan.validateOps.length;

  const lines: string[] = [];

  // Header
  lines.push(`-- schema-flow migration | ${ts} | v${version} | ${pgSchema}`);
  lines.push(`-- Tables: ${tableList} | Operations: ${totalOps}`);
  lines.push("");

  // Functions
  if (functions.length > 0) {
    lines.push("BEGIN;");
    lines.push("-- Functions");
    for (const fn of functions) {
      const replaceClause = fn.replace ? "OR REPLACE " : "";
      const argsClause = fn.args ? `(${fn.args})` : "()";
      const qualifiers: string[] = [];
      qualifiers.push(`LANGUAGE ${fn.language}`);
      if (fn.volatility) qualifiers.push(fn.volatility.toUpperCase());
      if (fn.security === "definer") qualifiers.push("SECURITY DEFINER");
      if (fn.parallel) qualifiers.push(`PARALLEL ${fn.parallel.toUpperCase()}`);
      if (fn.strict) qualifiers.push("STRICT");
      if (fn.leakproof) qualifiers.push("LEAKPROOF");
      if (fn.cost !== undefined) qualifiers.push(`COST ${fn.cost}`);
      if (fn.rows !== undefined) qualifiers.push(`ROWS ${fn.rows}`);
      if (fn.set) {
        for (const [key, val] of Object.entries(fn.set)) {
          qualifiers.push(`SET ${key} = ${val}`);
        }
      }
      lines.push(
        `CREATE ${replaceClause}FUNCTION ${fn.name}${argsClause} RETURNS ${fn.returns} ${qualifiers.join(" ")} AS $fn_body$`,
      );
      lines.push(fn.body);
      lines.push("$fn_body$;");
      lines.push("");
    }
    lines.push("COMMIT;");
    lines.push("");
  }

  // Separate index ops from non-index structure ops
  const indexOps = plan.structureOps.filter((o) => o.type === "add_index" || o.type === "add_unique_index");
  const nonIndexOps = plan.structureOps.filter((o) => o.type !== "add_index" && o.type !== "add_unique_index");

  // Structure ops (non-index) in a transaction
  if (nonIndexOps.length > 0) {
    lines.push("BEGIN;");
    lines.push("-- Structure");
    for (const op of nonIndexOps) {
      lines.push(`-- ${op.description}`);
      lines.push(op.sql);
      lines.push("");
    }
    lines.push("COMMIT;");
    lines.push("");
  }

  // Index ops outside transaction (CONCURRENTLY)
  if (indexOps.length > 0) {
    lines.push("-- Indexes (outside txn — CONCURRENTLY)");
    for (const op of indexOps) {
      lines.push(`-- ${op.description}`);
      lines.push(op.sql);
      lines.push("");
    }
  }

  // Foreign key ops in a transaction
  if (plan.foreignKeyOps.length > 0) {
    lines.push("BEGIN;");
    lines.push("-- Foreign keys");
    for (const op of plan.foreignKeyOps) {
      lines.push(`-- ${op.description}`);
      lines.push(op.sql);
      lines.push("");
    }
    lines.push("COMMIT;");
    lines.push("");
  }

  // Validate ops outside transaction
  if (plan.validateOps.length > 0) {
    lines.push("-- Validate (outside txn)");
    for (const op of plan.validateOps) {
      lines.push(`-- ${op.description}`);
      lines.push(op.sql);
      lines.push("");
    }
  }

  return lines.join("\n");
}

/** Generate a SQL file for a migration plan */
export function generateSqlFile(
  plan: MigrationPlan,
  functions: FunctionSchema[],
  options: {
    outputDir: string;
    name?: string;
    pgSchema?: string;
    version?: string;
  },
): SqlGenerateResult {
  const ts = utcTimestamp();
  const name = options.name || "migration";
  const filename = `${ts}_${name.replace(/[^a-zA-Z0-9_-]/g, "_").toLowerCase()}.sql`;

  if (!existsSync(options.outputDir)) {
    mkdirSync(options.outputDir, { recursive: true });
  }

  const filePath = path.join(options.outputDir, filename);

  const sql = formatMigrationSql(plan, functions, {
    timestamp: new Date().toISOString(),
    version: options.version,
    pgSchema: options.pgSchema,
  });

  const totalOps = functions.length + plan.structureOps.length + plan.foreignKeyOps.length + plan.validateOps.length;

  writeFileSync(filePath, sql, "utf-8");

  return { sql, filePath, operationCount: totalOps };
}
