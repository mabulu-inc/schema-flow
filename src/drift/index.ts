// src/drift/index.ts
// Drift detection: compare live DB state vs. YAML schema definitions

import path from "node:path";
import type { SchemaFlowConfig } from "../core/config.js";
import type {
  TableSchema,
  EnumSchema,
  ViewSchema,
  MaterializedViewSchema,
  UniqueConstraintDef,
} from "../schema/types.js";
import {
  parseTableFile,
  parseFunctionFile,
  parseEnumFile,
  parseExtensionsFile,
  parseViewFile,
  parseMaterializedViewFile,
} from "../schema/parser.js";
import { loadMixins, expandMixins } from "../schema/mixins.js";
import { discoverSchemaFiles } from "../core/files.js";
import { normalizeType } from "../planner/index.js";
import { withClient } from "../core/db.js";
import {
  getExistingTables,
  introspectTable,
  getExistingFunctions,
  getExistingEnums,
  getExistingExtensions,
  getExistingViews,
  getExistingMaterializedViews,
  getTableIndexes,
  getTableConstraints,
  parseIndexDefFull,
  getTableComment,
  getColumnComments,
  getEnumComment,
  getViewComment,
  getMaterializedViewComment,
  getFunctionComment,
  getIndexComments,
  getTriggerComments,
  getConstraintComments,
  getPolicyComments,
  type DbIndex,
} from "../introspect/index.js";
import type { FunctionSchema } from "../schema/types.js";

export interface DriftItem {
  category:
    | "table"
    | "column"
    | "index"
    | "constraint"
    | "trigger"
    | "policy"
    | "rls"
    | "function"
    | "enum"
    | "extension"
    | "view"
    | "materialized_view"
    | "comment";
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

  // Classify files
  const functionFiles: string[] = [];
  const tableFiles: string[] = [];
  const enumFiles: string[] = [];
  const extensionFiles: string[] = [];
  const viewFiles: string[] = [];
  const mvFiles: string[] = [];

  for (const f of schemaFiles) {
    const base = path.basename(f);
    if (base.startsWith("fn_")) functionFiles.push(f);
    else if (base.startsWith("enum_")) enumFiles.push(f);
    else if (base === "extensions.yaml" || base === "extensions.yml") extensionFiles.push(f);
    else if (base.startsWith("view_")) viewFiles.push(f);
    else if (base.startsWith("mv_")) mvFiles.push(f);
    else tableFiles.push(f);
  }

  // Parse schemas
  const parsedSchemas = tableFiles.map((f) => parseTableFile(f));
  const mixinMap = await loadMixins(config.mixinsDir);
  const expandedSchemas = expandMixins(parsedSchemas, mixinMap);
  const desiredMap = new Map(expandedSchemas.map((s) => [s.table, s]));

  // Parse functions
  const parsedFunctions: FunctionSchema[] = functionFiles.map((f) => parseFunctionFile(f));
  const desiredFuncMap = new Map(parsedFunctions.map((f) => [f.name, f]));

  // Parse enums
  const parsedEnums: EnumSchema[] = enumFiles.map((f) => parseEnumFile(f));
  const desiredEnumMap = new Map(parsedEnums.map((e) => [e.name, e]));

  // Parse extensions
  const desiredExtensions = new Set<string>();
  for (const f of extensionFiles) {
    const ext = parseExtensionsFile(f);
    ext.extensions.forEach((e) => desiredExtensions.add(e));
  }

  // Parse views
  const parsedViews: ViewSchema[] = viewFiles.map((f) => parseViewFile(f));
  const desiredViewMap = new Map(parsedViews.map((v) => [v.name, v]));

  // Parse materialized views
  const parsedMvs: MaterializedViewSchema[] = mvFiles.map((f) => parseMaterializedViewFile(f));
  const desiredMvMap = new Map(parsedMvs.map((v) => [v.name, v]));

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

      // Index diff
      const dbIndexes = await getTableIndexes(client, desired.table, config.pgSchema);
      diffTableIndexes(desired, dbIndexes, items);

      // Multi-column unique constraint diff
      if (desired.unique_constraints) {
        const dbConstraintsForUc = await getTableConstraints(client, desired.table, config.pgSchema);
        diffUniqueConstraints(desired, dbConstraintsForUc, items);
      }

      // Comment diff
      const tableComment = await getTableComment(client, desired.table, config.pgSchema);
      if (desired.comment !== undefined && desired.comment !== tableComment) {
        items.push({
          category: "comment",
          direction: "mismatch",
          table: desired.table,
          name: "table_comment",
          description: `Table comment on "${desired.table}" differs`,
          details: [{ field: "comment", expected: desired.comment, actual: tableComment || "(none)" }],
        });
      }
      const colComments = await getColumnComments(client, desired.table, config.pgSchema);
      for (const col of desired.columns) {
        if (col.comment !== undefined) {
          const actual = colComments.get(col.name) || null;
          if (col.comment !== actual) {
            items.push({
              category: "comment",
              direction: "mismatch",
              table: desired.table,
              name: col.name,
              description: `Column comment on "${desired.table}.${col.name}" differs`,
              details: [{ field: "comment", expected: col.comment, actual: actual || "(none)" }],
            });
          }
        }
      }

      // Index comment drift
      const indexComments = await getIndexComments(client, desired.table, config.pgSchema);
      for (const idx of desired.indexes || []) {
        if (idx.comment !== undefined) {
          const idxName = idx.name || `idx_${desired.table}_${idx.columns.join("_")}`;
          const actual = indexComments.get(idxName) || null;
          if (idx.comment !== actual) {
            items.push({
              category: "comment",
              direction: "mismatch",
              table: desired.table,
              name: idxName,
              description: `Index comment on "${idxName}" differs`,
              details: [{ field: "comment", expected: idx.comment, actual: actual || "(none)" }],
            });
          }
        }
      }

      // Trigger comment drift
      const triggerComments = await getTriggerComments(client, desired.table, config.pgSchema);
      for (const trigger of desired.triggers || []) {
        if (trigger.comment !== undefined) {
          const actual = triggerComments.get(trigger.name) || null;
          if (trigger.comment !== actual) {
            items.push({
              category: "comment",
              direction: "mismatch",
              table: desired.table,
              name: trigger.name,
              description: `Trigger comment on "${trigger.name}" differs`,
              details: [{ field: "comment", expected: trigger.comment, actual: actual || "(none)" }],
            });
          }
        }
      }

      // Constraint comment drift (checks)
      const constraintComments = await getConstraintComments(client, desired.table, config.pgSchema);
      for (const check of desired.checks || []) {
        if (check.comment !== undefined && check.name) {
          const actual = constraintComments.get(check.name) || null;
          if (check.comment !== actual) {
            items.push({
              category: "comment",
              direction: "mismatch",
              table: desired.table,
              name: check.name,
              description: `Constraint comment on "${check.name}" differs`,
              details: [{ field: "comment", expected: check.comment, actual: actual || "(none)" }],
            });
          }
        }
      }

      // Policy comment drift
      const policyComments = await getPolicyComments(client, desired.table, config.pgSchema);
      for (const policy of desired.policies || []) {
        if (policy.comment !== undefined) {
          const actual = policyComments.get(policy.name) || null;
          if (policy.comment !== actual) {
            items.push({
              category: "comment",
              direction: "mismatch",
              table: desired.table,
              name: policy.name,
              description: `Policy comment on "${policy.name}" differs`,
              details: [{ field: "comment", expected: policy.comment, actual: actual || "(none)" }],
            });
          }
        }
      }
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
      } else if (fn.comment !== undefined) {
        const currentComment = await getFunctionComment(client, fn.name, config.pgSchema);
        if (fn.comment !== currentComment) {
          items.push({
            category: "comment",
            direction: "mismatch",
            name: fn.name,
            description: `Function comment on "${fn.name}" differs`,
            details: [{ field: "comment", expected: fn.comment, actual: currentComment || "(none)" }],
          });
        }
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

    // Diff enums
    const existingEnums = await getExistingEnums(client, config.pgSchema);
    const existingEnumMap = new Map(existingEnums.map((e) => [e.name, e]));

    for (const desired of parsedEnums) {
      const existing = existingEnumMap.get(desired.name);
      if (!existing) {
        items.push({
          category: "enum",
          direction: "missing_from_db",
          name: desired.name,
          description: `Enum "${desired.name}" defined in YAML but does not exist in database`,
        });
      } else {
        // Check values
        const missingValues = desired.values.filter((v) => !existing.values.includes(v));
        const extraValues = existing.values.filter((v) => !desired.values.includes(v));
        if (missingValues.length > 0 || extraValues.length > 0) {
          items.push({
            category: "enum",
            direction: "mismatch",
            name: desired.name,
            description: `Enum "${desired.name}" values differ`,
            details: [
              ...(missingValues.length > 0
                ? [{ field: "missing_values", expected: missingValues.join(", "), actual: "(not in DB)" }]
                : []),
              ...(extraValues.length > 0
                ? [{ field: "extra_values", expected: "(not in YAML)", actual: extraValues.join(", ") }]
                : []),
            ],
          });
        }

        // Enum comment drift
        if (desired.comment !== undefined) {
          const currentComment = await getEnumComment(client, desired.name, config.pgSchema);
          if (desired.comment !== currentComment) {
            items.push({
              category: "comment",
              direction: "mismatch",
              name: desired.name,
              description: `Enum comment on "${desired.name}" differs`,
              details: [{ field: "comment", expected: desired.comment, actual: currentComment || "(none)" }],
            });
          }
        }
      }
    }
    for (const existing of existingEnums) {
      if (!desiredEnumMap.has(existing.name)) {
        items.push({
          category: "enum",
          direction: "extra_in_db",
          name: existing.name,
          description: `Enum "${existing.name}" exists in database but has no schema file`,
        });
      }
    }

    // Diff extensions
    const existingExtSet = new Set(await getExistingExtensions(client));
    for (const ext of desiredExtensions) {
      if (!existingExtSet.has(ext)) {
        items.push({
          category: "extension",
          direction: "missing_from_db",
          name: ext,
          description: `Extension "${ext}" defined in YAML but not installed`,
        });
      }
    }
    for (const ext of existingExtSet) {
      if (!desiredExtensions.has(ext) && desiredExtensions.size > 0) {
        items.push({
          category: "extension",
          direction: "extra_in_db",
          name: ext,
          description: `Extension "${ext}" installed but not in schema file`,
        });
      }
    }

    // Diff views
    const existingViews = await getExistingViews(client, config.pgSchema);
    const existingViewMap = new Map(existingViews.map((v) => [v.name, v]));
    for (const desired of parsedViews) {
      const existing = existingViewMap.get(desired.name);
      if (!existing) {
        items.push({
          category: "view",
          direction: "missing_from_db",
          name: desired.name,
          description: `View "${desired.name}" defined in YAML but does not exist in database`,
        });
      } else {
        const desiredNorm = desired.query.replace(/\s+/g, " ").trim();
        const existingNorm = existing.query.replace(/\s+/g, " ").trim();
        if (desiredNorm !== existingNorm) {
          items.push({
            category: "view",
            direction: "mismatch",
            name: desired.name,
            description: `View "${desired.name}" query differs from database`,
          });
        }

        // View comment drift
        if (desired.comment !== undefined) {
          const currentComment = await getViewComment(client, desired.name, config.pgSchema);
          if (desired.comment !== currentComment) {
            items.push({
              category: "comment",
              direction: "mismatch",
              name: desired.name,
              description: `View comment on "${desired.name}" differs`,
              details: [{ field: "comment", expected: desired.comment, actual: currentComment || "(none)" }],
            });
          }
        }
      }
    }
    for (const existing of existingViews) {
      if (!desiredViewMap.has(existing.name)) {
        items.push({
          category: "view",
          direction: "extra_in_db",
          name: existing.name,
          description: `View "${existing.name}" exists in database but has no schema file`,
        });
      }
    }

    // Diff materialized views
    const existingMvs = await getExistingMaterializedViews(client, config.pgSchema);
    const existingMvMap = new Map(existingMvs.map((v) => [v.name, v]));
    for (const desired of parsedMvs) {
      const existing = existingMvMap.get(desired.name);
      if (!existing) {
        items.push({
          category: "materialized_view",
          direction: "missing_from_db",
          name: desired.name,
          description: `Materialized view "${desired.name}" defined in YAML but does not exist in database`,
        });
      } else {
        const desiredNorm = desired.query.replace(/\s+/g, " ").trim();
        const existingNorm = existing.query.replace(/\s+/g, " ").trim();
        if (desiredNorm !== existingNorm) {
          items.push({
            category: "materialized_view",
            direction: "mismatch",
            name: desired.name,
            description: `Materialized view "${desired.name}" query differs from database`,
          });
        }

        // Materialized view comment drift
        if (desired.comment !== undefined) {
          const currentComment = await getMaterializedViewComment(client, desired.name, config.pgSchema);
          if (desired.comment !== currentComment) {
            items.push({
              category: "comment",
              direction: "mismatch",
              name: desired.name,
              description: `Materialized view comment on "${desired.name}" differs`,
              details: [{ field: "comment", expected: desired.comment, actual: currentComment || "(none)" }],
            });
          }
        }
      }
    }
    for (const existing of existingMvs) {
      if (!desiredMvMap.has(existing.name)) {
        items.push({
          category: "materialized_view",
          direction: "extra_in_db",
          name: existing.name,
          description: `Materialized view "${existing.name}" exists in database but has no schema file`,
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
        (isSerial(col.type) && isIntegerEquiv(existing.type)) || (isSerial(existing.type) && isIntegerEquiv(col.type));
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
      const eNorm = String(existing.default)
        .replace(/'/g, "")
        .replace(/::[^)]+$/, "")
        .trim();
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

    // Unique constraint name
    if (
      col.unique &&
      existing.unique &&
      col.unique_name &&
      existing.unique_name &&
      col.unique_name !== existing.unique_name
    ) {
      details.push({ field: "unique_name", expected: col.unique_name, actual: existing.unique_name });
    }

    // FK constraint name
    if (
      col.references &&
      existing.references &&
      col.references.name &&
      existing.references.name &&
      col.references.name !== existing.references.name
    ) {
      details.push({ field: "fk_name", expected: col.references.name, actual: existing.references.name });
    }

    // FK deferrable
    if (col.references && existing.references) {
      const desiredDeferrable = col.references.deferrable === true;
      const existingDeferrable = existing.references.deferrable === true;
      if (desiredDeferrable !== existingDeferrable) {
        details.push({ field: "deferrable", expected: String(desiredDeferrable), actual: String(existingDeferrable) });
      }
      const desiredDeferred = col.references.initially_deferred === true;
      const existingDeferred = existing.references.initially_deferred === true;
      if (desiredDeferred !== existingDeferred) {
        details.push({
          field: "initially_deferred",
          expected: String(desiredDeferred),
          actual: String(existingDeferred),
        });
      }
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

  // PK constraint name comparison
  if (desired.primary_key_name && current.primary_key_name && desired.primary_key_name !== current.primary_key_name) {
    items.push({
      category: "constraint",
      direction: "mismatch",
      table: tableName,
      name: "primary_key",
      description: `PK constraint name on "${tableName}" differs`,
      details: [{ field: "primary_key_name", expected: desired.primary_key_name, actual: current.primary_key_name }],
    });
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

function diffTableIndexes(desired: TableSchema, dbIndexes: DbIndex[], items: DriftItem[]): void {
  const tableName = desired.table;
  const desiredIndexes = desired.indexes || [];

  // Filter out constraint-backing indexes using semantic metadata
  const existing = dbIndexes.filter((i) => !i.constraint_type).map((i) => parseIndexDefFull(i.indexdef));

  const existingByName = new Map(existing.map((i) => [i.name, i]));
  const desiredByName = new Map<string, unknown>();
  for (const idx of desiredIndexes) {
    const name = idx.name || `idx_${tableName}_${idx.columns.join("_")}`;
    desiredByName.set(name, idx);
  }

  // Extra indexes in DB
  for (const [name] of existingByName) {
    if (!desiredByName.has(name)) {
      items.push({
        category: "index",
        direction: "extra_in_db",
        table: tableName,
        name,
        description: `Index "${name}" on "${tableName}" exists in database but not in YAML`,
      });
    }
  }

  // Missing indexes from DB
  for (const [name] of desiredByName) {
    if (!existingByName.has(name)) {
      items.push({
        category: "index",
        direction: "missing_from_db",
        table: tableName,
        name,
        description: `Index "${name}" on "${tableName}" defined in YAML but not in database`,
      });
    }
  }
}

function diffUniqueConstraints(
  desired: TableSchema,
  dbConstraints: { constraint_name: string; constraint_type: string; column_name: string }[],
  items: DriftItem[],
): void {
  const tableName = desired.table;
  const desiredUcs = desired.unique_constraints || [];

  // Build map of existing multi-column unique constraints from DB
  const existingUcMap = new Map<string, string[]>();
  for (const c of dbConstraints) {
    if (c.constraint_type === "UNIQUE") {
      if (!existingUcMap.has(c.constraint_name)) existingUcMap.set(c.constraint_name, []);
      existingUcMap.get(c.constraint_name)!.push(c.column_name);
    }
  }
  // Only keep multi-column unique constraints
  for (const [name, cols] of existingUcMap) {
    if (cols.length <= 1) existingUcMap.delete(name);
  }

  const desiredByName = new Map<string, UniqueConstraintDef>();
  for (const uc of desiredUcs) {
    const name = uc.name || `uq_${tableName}_${uc.columns.join("_")}`;
    desiredByName.set(name, uc);
  }

  // Extra in DB
  for (const [name] of existingUcMap) {
    if (!desiredByName.has(name)) {
      items.push({
        category: "constraint",
        direction: "extra_in_db",
        table: tableName,
        name,
        description: `Unique constraint "${name}" on "${tableName}" exists in database but not in YAML`,
      });
    }
  }

  // Missing from DB
  for (const [name] of desiredByName) {
    if (!existingUcMap.has(name)) {
      items.push({
        category: "constraint",
        direction: "missing_from_db",
        table: tableName,
        name,
        description: `Unique constraint "${name}" on "${tableName}" defined in YAML but not in database`,
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
