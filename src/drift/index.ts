// src/drift/index.ts
// Drift detection: compare live DB state vs. YAML schema definitions

import pg from "pg";
import path from "node:path";
import type { SchemaFlowConfig } from "../core/config.js";
import type { TableSchema, ColumnDef } from "../schema/types.js";
import { parseTableFile, parseFunctionFile } from "../schema/parser.js";
import { loadMixins, expandMixins } from "../schema/mixins.js";
import { discoverSchemaFiles } from "../core/files.js";
import { normalizeType } from "../planner/index.js";
import { withClient } from "../core/db.js";
import {
  getExistingTables,
  introspectTable,
  getExistingFunctions,
} from "../introspect/index.js";
import type { FunctionSchema } from "../schema/types.js";

export interface DriftItem {
  category: "table" | "column" | "index" | "constraint" | "trigger" | "policy" | "rls" | "function";
  direction: "extra_in_db" | "missing_from_db" | "mismatch";
  table?: string;
  name: string;
  description: string;
  details?: { field: string; expected: string; actual: string }[];
}

export interface DriftReport {
  items: DriftItem[];
  hasDrift: boolean;
  summary: {
    extraInDb: number;
    missingFromDb: number;
    mismatches: number;
    tablesChecked: number;
    functionsChecked: number;
  };
}

export async function detectDrift(config: SchemaFlowConfig): Promise<DriftReport> {
  const items: DriftItem[] = [];

  const schemaFiles = await discoverSchemaFiles(config.schemaDir);
  const functionFiles = schemaFiles.filter((f) => path.basename(f).startsWith("fn_"));
  const tableFiles = schemaFiles.filter((f) => !path.basename(f).startsWith("fn_"));

  // Parse schemas
  const parsedSchemas = tableFiles.map((f) => parseTableFile(f));
  const mixinMap = await loadMixins(config.mixinsDir);
  const expandedSchemas = expandMixins(parsedSchemas, mixinMap);
  const desiredMap = new Map(expandedSchemas.map((s) => [s.table, s]));

  // Parse functions
  const parsedFunctions: FunctionSchema[] = functionFiles.map((f) => parseFunctionFile(f));
  const desiredFuncMap = new Map(parsedFunctions.map((f) => [f.name, f]));

  let tablesChecked = 0;
  let functionsChecked = 0;

  await withClient(config.connectionString, async (client) => {
    const existingTables = await getExistingTables(client, config.pgSchema);
    const existingTableSet = new Set(existingTables);

    // Check for tables in YAML but not in DB
    for (const schema of expandedSchemas) {
      if (!existingTableSet.has(schema.table)) {
        items.push({
          category: "table",
          direction: "missing_from_db",
          name: schema.table,
          description: `Table "${schema.table}" defined in YAML but does not exist in database`,
        });
      }
    }

    // Check for tables in DB but not in YAML
    for (const tableName of existingTables) {
      if (tableName.startsWith("_schema_flow") || tableName.startsWith("pg_")) continue;
      if (!desiredMap.has(tableName)) {
        items.push({
          category: "table",
          direction: "extra_in_db",
          name: tableName,
          description: `Table "${tableName}" exists in database but has no schema file`,
        });
      }
    }

    // Diff each declared table that exists in DB
    for (const desired of expandedSchemas) {
      if (!existingTableSet.has(desired.table)) continue;
      tablesChecked++;

      const current = await introspectTable(client, desired.table, config.pgSchema);
      diffTable(desired, current, items);
    }

    // Diff functions
    const existingFunctions = await getExistingFunctions(client, config.pgSchema);
    const existingFuncSet = new Set(existingFunctions.map((f) => f.routine_name));

    for (const fn of parsedFunctions) {
      functionsChecked++;
      if (!existingFuncSet.has(fn.name)) {
        items.push({
          category: "function",
          direction: "missing_from_db",
          name: fn.name,
          description: `Function "${fn.name}" defined in YAML but does not exist in database`,
        });
      }
    }

    for (const fn of existingFunctions) {
      if (fn.routine_name.startsWith("_sf_")) continue;
      if (!desiredFuncMap.has(fn.routine_name)) {
        items.push({
          category: "function",
          direction: "extra_in_db",
          name: fn.routine_name,
          description: `Function "${fn.routine_name}" exists in database but has no schema file`,
        });
      }
    }
  });

  const extraInDb = items.filter((i) => i.direction === "extra_in_db").length;
  const missingFromDb = items.filter((i) => i.direction === "missing_from_db").length;
  const mismatches = items.filter((i) => i.direction === "mismatch").length;

  return {
    items,
    hasDrift: items.length > 0,
    summary: { extraInDb, missingFromDb, mismatches, tablesChecked, functionsChecked },
  };
}

function diffTable(desired: TableSchema, current: TableSchema, items: DriftItem[]): void {
  const tableName = desired.table;
  const currentColMap = new Map(current.columns.map((c) => [c.name, c]));
  const desiredColMap = new Map(desired.columns.map((c) => [c.name, c]));

  // Extra columns in DB
  for (const col of current.columns) {
    if (!desiredColMap.has(col.name)) {
      items.push({
        category: "column",
        direction: "extra_in_db",
        table: tableName,
        name: col.name,
        description: `Column "${tableName}.${col.name}" exists in database but not in YAML`,
      });
    }
  }

  // Missing columns in DB
  for (const col of desired.columns) {
    if (!currentColMap.has(col.name)) {
      items.push({
        category: "column",
        direction: "missing_from_db",
        table: tableName,
        name: col.name,
        description: `Column "${tableName}.${col.name}" defined in YAML but not in database`,
      });
      continue;
    }

    // Column exists — check for mismatches
    const existing = currentColMap.get(col.name)!;
    const details: DriftItem["details"] = [];

    // Type comparison
    if (normalizeType(existing.type) !== normalizeType(col.type)) {
      // Skip serial vs integer mismatch (serial is just integer + sequence)
      const isSerialMatch =
        (isSerial(col.type) && isIntegerEquiv(existing.type)) ||
        (isSerial(existing.type) && isIntegerEquiv(col.type));
      if (!isSerialMatch) {
        details.push({
          field: "type",
          expected: col.type,
          actual: existing.type,
        });
      }
    }

    // Nullable comparison
    const desiredNullable = col.nullable === true;
    const existingNullable = existing.nullable === true;
    if (desiredNullable !== existingNullable) {
      details.push({
        field: "nullable",
        expected: String(desiredNullable),
        actual: String(existingNullable),
      });
    }

    // Default comparison (rough — defaults can vary in representation)
    if (col.default !== undefined && existing.default !== undefined) {
      const dNorm = String(col.default).replace(/'/g, "").trim();
      const eNorm = String(existing.default).replace(/'/g, "").replace(/::[^)]+$/, "").trim();
      if (dNorm !== eNorm && col.default !== existing.default) {
        details.push({
          field: "default",
          expected: String(col.default),
          actual: String(existing.default),
        });
      }
    } else if (col.default !== undefined && existing.default === undefined) {
      details.push({ field: "default", expected: String(col.default), actual: "(none)" });
    }

    // Unique
    if (col.unique && !existing.unique) {
      details.push({ field: "unique", expected: "true", actual: "false" });
    } else if (!col.unique && existing.unique) {
      details.push({ field: "unique", expected: "false", actual: "true" });
    }

    if (details.length > 0) {
      items.push({
        category: "column",
        direction: "mismatch",
        table: tableName,
        name: col.name,
        description: `Column "${tableName}.${col.name}" differs: ${details.map((d) => d.field).join(", ")}`,
        details,
      });
    }
  }

  // Trigger diff
  const desiredTriggers = new Map((desired.triggers || []).map((t) => [t.name, t]));
  const currentTriggers = new Map((current.triggers || []).map((t) => [t.name, t]));

  for (const [name] of currentTriggers) {
    if (!desiredTriggers.has(name)) {
      items.push({
        category: "trigger",
        direction: "extra_in_db",
        table: tableName,
        name,
        description: `Trigger "${name}" on "${tableName}" exists in database but not in YAML`,
      });
    }
  }
  for (const [name] of desiredTriggers) {
    if (!currentTriggers.has(name)) {
      items.push({
        category: "trigger",
        direction: "missing_from_db",
        table: tableName,
        name,
        description: `Trigger "${name}" on "${tableName}" defined in YAML but not in database`,
      });
    }
  }

  // RLS diff
  const desiredRls = desired.rls === true;
  const currentRls = current.rls === true;
  if (desiredRls !== currentRls) {
    items.push({
      category: "rls",
      direction: "mismatch",
      table: tableName,
      name: "rls",
      description: `RLS on "${tableName}": expected ${desiredRls ? "enabled" : "disabled"}, actual ${currentRls ? "enabled" : "disabled"}`,
      details: [{ field: "rls", expected: String(desiredRls), actual: String(currentRls) }],
    });
  }

  // Policy diff
  const desiredPolicies = new Map((desired.policies || []).map((p) => [p.name, p]));
  const currentPolicies = new Map((current.policies || []).map((p) => [p.name, p]));

  for (const [name] of currentPolicies) {
    if (!desiredPolicies.has(name)) {
      items.push({
        category: "policy",
        direction: "extra_in_db",
        table: tableName,
        name,
        description: `Policy "${name}" on "${tableName}" exists in database but not in YAML`,
      });
    }
  }
  for (const [name] of desiredPolicies) {
    if (!currentPolicies.has(name)) {
      items.push({
        category: "policy",
        direction: "missing_from_db",
        table: tableName,
        name,
        description: `Policy "${name}" on "${tableName}" defined in YAML but not in database`,
      });
    }
  }
}

function isSerial(t: string): boolean {
  return ["serial", "bigserial", "smallserial"].includes(t.toLowerCase());
}

function isIntegerEquiv(t: string): boolean {
  return ["integer", "bigint", "smallint", "int", "int4", "int8", "int2"].includes(t.toLowerCase());
}
