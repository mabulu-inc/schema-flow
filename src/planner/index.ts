// src/planner/index.ts
// Diff engine: compare desired YAML state with current DB state, produce SQL operations
// Key design: CREATE TABLEs first (without FKs), then ALTER TABLEs for FKs last
// Safety: destructive operations (drops, narrowing changes) are blocked by default

import type { TableSchema, ColumnDef, IndexDef, CheckDef, TriggerDef, PolicyDef } from "../schema/types.js";
import { introspectTable, getExistingTables, getTableConstraints } from "../introspect/index.js";
import { logger } from "../core/logger.js";
import pg from "pg";

export type OperationType =
  | "create_table"
  | "add_column"
  | "alter_column"
  | "drop_column"
  | "add_index"
  | "add_unique_index"
  | "drop_index"
  | "add_check"
  | "add_check_not_valid"
  | "add_foreign_key"
  | "add_foreign_key_not_valid"
  | "validate_constraint"
  | "drop_foreign_key"
  | "drop_table"
  | "create_function"
  | "create_trigger"
  | "drop_trigger"
  | "enable_rls"
  | "disable_rls"
  | "create_policy"
  | "drop_policy"
  | "expand_column"
  | "create_dual_write_trigger"
  | "backfill_column"
  | "contract_column"
  | "drop_dual_write_trigger";

export interface Operation {
  type: OperationType;
  table?: string;
  sql: string;
  description: string;
  phase: "structure" | "foreign_key" | "validate";
  /** Whether this operation is destructive (data loss, breaking change) */
  destructive: boolean;
  /** Grouping key for related operations (e.g., safe NOT NULL 4-step) */
  group?: string;
  /** Arbitrary metadata for operation-specific data */
  meta?: Record<string, unknown>;
}

export interface MigrationPlan {
  operations: Operation[];
  structureOps: Operation[];
  foreignKeyOps: Operation[];
  /** Validate-phase operations (VALIDATE CONSTRAINT, SET NOT NULL, cleanup) */
  validateOps: Operation[];
  /** Destructive operations that were blocked (safe mode) */
  blocked: Operation[];
  summary: {
    tablesToCreate: string[];
    tablesToAlter: string[];
    foreignKeysToAdd: number;
    validateOpsCount: number;
    totalOperations: number;
    destructiveCount: number;
    blockedCount: number;
  };
}

/** Options controlling plan behavior */
export interface PlanOptions {
  /** If true, destructive operations are included in the plan. Otherwise they are blocked. */
  allowDestructive?: boolean;
}

export async function buildPlan(
  client: pg.PoolClient,
  desiredSchemas: TableSchema[],
  pgSchema: string,
  options: PlanOptions = {},
): Promise<MigrationPlan> {
  const allowDestructive = options.allowDestructive ?? false;
  const existingTables = new Set(await getExistingTables(client, pgSchema));
  const desiredTableNames = new Set(desiredSchemas.map((s) => s.table));
  const allOps: Operation[] = [];

  for (const desired of desiredSchemas) {
    if (!existingTables.has(desired.table)) {
      // CREATE TABLE (without foreign keys — those come later)
      const createOps = planCreateTable(desired, pgSchema);
      allOps.push(...createOps);
    } else {
      // ALTER TABLE — diff columns, indexes, checks
      const alterOps = await planAlterTable(client, desired, pgSchema);
      allOps.push(...alterOps);
    }
  }

  // Detect tables in DB that have no corresponding schema file (orphan detection)
  for (const existingTable of existingTables) {
    // Skip the history table and internal PG tables
    if (existingTable.startsWith("_schema_flow") || existingTable.startsWith("pg_")) continue;

    if (!desiredTableNames.has(existingTable)) {
      logger.warn(
        `Table "${existingTable}" exists in the database but has no schema file. ` +
          `To drop it, use --allow-destructive. To keep it, create a schema file for it ` +
          `(run: schema-flow generate).`,
      );
      // We do NOT auto-generate DROP TABLE ops. The user must explicitly request that
      // by adding a future feature or handling it in a pre/post script.
      // This is intentional: accidentally dropping a table is catastrophic.
    }
  }

  // Separate safe from destructive, and filter blocked ops
  const allowed: Operation[] = [];
  const blocked: Operation[] = [];

  for (const op of allOps) {
    if (op.destructive && !allowDestructive) {
      blocked.push(op);
    } else {
      allowed.push(op);
    }
  }

  if (blocked.length > 0) {
    logger.warn(
      `${blocked.length} destructive operation(s) blocked (safe mode). ` + `Use --allow-destructive to apply them.`,
    );
    for (const op of blocked) {
      logger.warn(`  BLOCKED: ${op.description}`);
    }
  }

  // Separate structure ops from FK ops and validate ops (only from allowed)
  const structureOps = allowed.filter((op) => op.phase === "structure");
  const foreignKeyOps = allowed.filter((op) => op.phase === "foreign_key");
  const validateOps = allowed.filter((op) => op.phase === "validate");

  const tablesToCreate = [...new Set(allowed.filter((o) => o.type === "create_table").map((o) => o.table!))];

  const tablesToAlter = [
    ...new Set(
      allowed
        .filter((o) => o.type !== "create_table" && o.table)
        .map((o) => o.table!)
        .filter((t) => !tablesToCreate.includes(t)),
    ),
  ];

  const destructiveCount = allowed.filter((o) => o.destructive).length;

  return {
    operations: [...structureOps, ...foreignKeyOps, ...validateOps],
    structureOps,
    foreignKeyOps,
    validateOps,
    blocked,
    summary: {
      tablesToCreate,
      tablesToAlter,
      foreignKeysToAdd: foreignKeyOps.length,
      validateOpsCount: validateOps.length,
      totalOperations: structureOps.length + foreignKeyOps.length + validateOps.length,
      destructiveCount,
      blockedCount: blocked.length,
    },
  };
}

function planCreateTable(schema: TableSchema, pgSchema: string): Operation[] {
  const ops: Operation[] = [];
  const fkOps: Operation[] = [];

  const colDefs: string[] = [];
  for (const col of schema.columns) {
    let def = `  "${col.name}" ${col.type}`;

    if (col.primary_key) def += " PRIMARY KEY";
    if (col.unique) def += " UNIQUE";
    if (col.nullable === false || col.primary_key) {
      // PRIMARY KEY implies NOT NULL; for explicit NOT NULL
      if (!col.primary_key) def += " NOT NULL";
    } else if (col.nullable === true) {
      // Nullable is the default, no need to add anything
    } else if (!col.primary_key) {
      def += " NOT NULL"; // Convention: NOT NULL by default
    }
    if (col.default !== undefined) def += ` DEFAULT ${col.default}`;

    colDefs.push(def);

    // Collect foreign keys for deferred application (NOT VALID + VALIDATE two-step)
    if (col.references) {
      const fkName = `fk_${schema.table}_${col.name}_${col.references.table}`;
      const onDelete = col.references.on_delete || "NO ACTION";
      const onUpdate = col.references.on_update || "NO ACTION";
      fkOps.push({
        type: "add_foreign_key_not_valid",
        table: schema.table,
        sql: `ALTER TABLE "${pgSchema}"."${schema.table}" ADD CONSTRAINT "${fkName}" FOREIGN KEY ("${col.name}") REFERENCES "${pgSchema}"."${col.references.table}" ("${col.references.column}") ON DELETE ${onDelete} ON UPDATE ${onUpdate} NOT VALID;`,
        description: `Add FK ${schema.table}.${col.name} → ${col.references.table}.${col.references.column} (NOT VALID)`,
        phase: "foreign_key",
        destructive: false,
      });
      fkOps.push({
        type: "validate_constraint",
        table: schema.table,
        sql: `ALTER TABLE "${pgSchema}"."${schema.table}" VALIDATE CONSTRAINT "${fkName}";`,
        description: `Validate FK ${fkName} on ${schema.table}`,
        phase: "validate",
        destructive: false,
      });
    }
  }

  // Composite primary key
  if (schema.primary_key && schema.primary_key.length > 0) {
    const pkCols = schema.primary_key.map((c) => `"${c}"`).join(", ");
    colDefs.push(`  PRIMARY KEY (${pkCols})`);
  }

  const createSql = `CREATE TABLE "${pgSchema}"."${schema.table}" (\n${colDefs.join(",\n")}\n);`;
  ops.push({
    type: "create_table",
    table: schema.table,
    sql: createSql,
    description: `Create table ${schema.table}`,
    phase: "structure",
    destructive: false,
  });

  // Indexes
  if (schema.indexes) {
    for (const idx of schema.indexes) {
      const idxOp = planCreateIndex(schema.table, idx, pgSchema);
      ops.push(idxOp);
    }
  }

  // Check constraints
  if (schema.checks) {
    for (const check of schema.checks) {
      const checkName = check.name || `chk_${schema.table}_${Date.now()}`;
      ops.push({
        type: "add_check",
        table: schema.table,
        sql: `ALTER TABLE "${pgSchema}"."${schema.table}" ADD CONSTRAINT "${checkName}" CHECK (${check.expression});`,
        description: `Add check constraint on ${schema.table}: ${check.expression}`,
        phase: "structure",
        destructive: false,
      });
    }
  }

  // Triggers
  if (schema.triggers) {
    for (const trigger of schema.triggers) {
      ops.push(planCreateTrigger(schema.table, trigger, pgSchema));
    }
  }

  // RLS
  if (schema.rls) {
    ops.push(...planEnableRLS(schema.table, pgSchema, schema.force_rls === true));
  }

  // Policies
  if (schema.policies) {
    for (const policy of schema.policies) {
      ops.push(planCreatePolicy(schema.table, policy, pgSchema));
    }
  }

  // FK ops come last
  ops.push(...fkOps);

  return ops;
}

async function planAlterTable(client: pg.PoolClient, desired: TableSchema, pgSchema: string): Promise<Operation[]> {
  const ops: Operation[] = [];
  const current = await introspectTable(client, desired.table, pgSchema);

  const currentColMap = new Map(current.columns.map((c) => [c.name, c]));
  const desiredColMap = new Map(desired.columns.map((c) => [c.name, c]));

  // Fetch existing constraints for FK validation state, CHECK diffing, etc.
  const dbConstraints = await getTableConstraints(client, desired.table, pgSchema);
  const existingFkMap = new Map<string, { convalidated: boolean }>();
  for (const c of dbConstraints) {
    if (c.constraint_type === "FOREIGN KEY") {
      existingFkMap.set(c.constraint_name, { convalidated: c.convalidated });
    }
  }

  // Track existing check constraints (excluding NOT NULL helpers)
  const existingCheckMap = new Map<string, string>();
  for (const c of dbConstraints) {
    if (c.constraint_type === "CHECK" && c.check_expression) {
      existingCheckMap.set(c.constraint_name, c.check_expression);
    }
  }

  // Add new columns
  for (const col of desired.columns) {
    if (!currentColMap.has(col.name)) {
      // Check for expand/contract pattern
      if (col.expand) {
        const { planExpandColumn } = await import("../expand/planner.js");
        const expandOps = planExpandColumn(desired.table, col.name, col.type, col.expand, pgSchema);
        ops.push(...expandOps);
        continue;
      }

      const addSql = buildAddColumnSql(desired.table, col, pgSchema);
      ops.push({
        type: "add_column",
        table: desired.table,
        sql: addSql,
        description: `Add column ${desired.table}.${col.name}`,
        phase: "structure",
        destructive: false,
      });

      // FK for new column (NOT VALID + VALIDATE two-step)
      if (col.references) {
        const fkName = `fk_${desired.table}_${col.name}_${col.references.table}`;
        const onDelete = col.references.on_delete || "NO ACTION";
        const onUpdate = col.references.on_update || "NO ACTION";
        ops.push({
          type: "add_foreign_key_not_valid",
          table: desired.table,
          sql: `ALTER TABLE "${pgSchema}"."${desired.table}" ADD CONSTRAINT "${fkName}" FOREIGN KEY ("${col.name}") REFERENCES "${pgSchema}"."${col.references.table}" ("${col.references.column}") ON DELETE ${onDelete} ON UPDATE ${onUpdate} NOT VALID;`,
          description: `Add FK ${desired.table}.${col.name} → ${col.references.table}.${col.references.column} (NOT VALID)`,
          phase: "foreign_key",
          destructive: false,
        });
        ops.push({
          type: "validate_constraint",
          table: desired.table,
          sql: `ALTER TABLE "${pgSchema}"."${desired.table}" VALIDATE CONSTRAINT "${fkName}";`,
          description: `Validate FK ${fkName} on ${desired.table}`,
          phase: "validate",
          destructive: false,
        });
      }

      // Safe UNIQUE for new column on existing table
      if (col.unique) {
        const idxName = `idx_${desired.table}_${col.name}_unique`;
        const constraintName = `uq_${desired.table}_${col.name}`;
        ops.push({
          type: "add_unique_index",
          table: desired.table,
          sql: `CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "${idxName}" ON "${pgSchema}"."${desired.table}" ("${col.name}");`,
          description: `Create unique index ${idxName} on ${desired.table}.${col.name}`,
          phase: "structure",
          destructive: false,
        });
        ops.push({
          type: "alter_column",
          table: desired.table,
          sql: `ALTER TABLE "${pgSchema}"."${desired.table}" ADD CONSTRAINT "${constraintName}" UNIQUE USING INDEX "${idxName}";`,
          description: `Add unique constraint ${constraintName} on ${desired.table}.${col.name} using index`,
          phase: "validate",
          destructive: false,
        });
      }

      continue;
    }

    // Alter existing columns if type/nullable/default/unique changed
    const existing = currentColMap.get(col.name)!;
    const alterOps = planAlterColumn(desired.table, existing, col, pgSchema, existingCheckMap);
    ops.push(...alterOps);

    // Handle FK diff for existing columns
    if (col.references) {
      const fkName = `fk_${desired.table}_${col.name}_${col.references.table}`;
      const existingFk = existingFkMap.get(fkName);
      if (existingFk) {
        // FK exists — if not validated, emit only VALIDATE
        if (!existingFk.convalidated) {
          ops.push({
            type: "validate_constraint",
            table: desired.table,
            sql: `ALTER TABLE "${pgSchema}"."${desired.table}" VALIDATE CONSTRAINT "${fkName}";`,
            description: `Validate FK ${fkName} on ${desired.table}`,
            phase: "validate",
            destructive: false,
          });
        }
        // If fully validated, no op needed
      } else if (!existing.references) {
        // FK doesn't exist yet — add with NOT VALID + VALIDATE
        const onDelete = col.references.on_delete || "NO ACTION";
        const onUpdate = col.references.on_update || "NO ACTION";
        ops.push({
          type: "add_foreign_key_not_valid",
          table: desired.table,
          sql: `ALTER TABLE "${pgSchema}"."${desired.table}" ADD CONSTRAINT "${fkName}" FOREIGN KEY ("${col.name}") REFERENCES "${pgSchema}"."${col.references.table}" ("${col.references.column}") ON DELETE ${onDelete} ON UPDATE ${onUpdate} NOT VALID;`,
          description: `Add FK ${desired.table}.${col.name} → ${col.references.table}.${col.references.column} (NOT VALID)`,
          phase: "foreign_key",
          destructive: false,
        });
        ops.push({
          type: "validate_constraint",
          table: desired.table,
          sql: `ALTER TABLE "${pgSchema}"."${desired.table}" VALIDATE CONSTRAINT "${fkName}";`,
          description: `Validate FK ${fkName} on ${desired.table}`,
          phase: "validate",
          destructive: false,
        });
      }
    }
  }

  // Columns in DB but NOT in the desired schema → destructive drop
  for (const existingCol of current.columns) {
    if (!desiredColMap.has(existingCol.name)) {
      ops.push({
        type: "drop_column",
        table: desired.table,
        sql: `ALTER TABLE "${pgSchema}"."${desired.table}" DROP COLUMN "${existingCol.name}";`,
        description: `Drop column ${desired.table}.${existingCol.name}`,
        phase: "structure",
        destructive: true,
      });
    }
  }

  // Check constraint diff for existing tables
  if (desired.checks) {
    const checkOps = planCheckDiff(desired.table, desired.checks, existingCheckMap, pgSchema);
    ops.push(...checkOps);
  }

  // Trigger diff
  const triggerOps = planTriggerDiff(desired.table, desired.triggers || [], current.triggers || [], pgSchema);
  ops.push(...triggerOps);

  // RLS diff
  const currentRls = { rls: current.rls === true, force_rls: current.force_rls === true };
  const rlsOps = planRLSDiff(desired.table, desired, currentRls, pgSchema);
  ops.push(...rlsOps);

  // Policy diff
  const policyOps = planPolicyDiff(desired.table, desired.policies || [], current.policies || [], pgSchema);
  ops.push(...policyOps);

  return ops;
}

function planAlterColumn(
  tableName: string,
  existing: ColumnDef,
  desired: ColumnDef,
  pgSchema: string,
  existingCheckMap?: Map<string, string>,
): Operation[] {
  const ops: Operation[] = [];
  const qualifiedTable = `"${pgSchema}"."${tableName}"`;

  // Type change
  if (normalizeType(existing.type) !== normalizeType(desired.type)) {
    // Skip type change for serial types (they're just integer + sequence)
    if (!isSerialType(desired.type) || !isIntegerType(existing.type)) {
      // Classify: widening is safe, narrowing is destructive
      const narrowing = isNarrowingTypeChange(existing.type, desired.type);
      ops.push({
        type: "alter_column",
        table: tableName,
        sql: `ALTER TABLE ${qualifiedTable} ALTER COLUMN "${desired.name}" TYPE ${desired.type} USING "${desired.name}"::${desired.type};`,
        description: `Change type of ${tableName}.${desired.name}: ${existing.type} → ${desired.type}`,
        phase: "structure",
        destructive: narrowing,
      });
    }
  }

  // Nullable change — use safe NOT NULL 4-step pattern (PG 12+)
  const existingNullable = existing.nullable === true;
  const desiredNullable = desired.nullable === true;
  if (existingNullable !== desiredNullable && !desired.primary_key) {
    if (desiredNullable) {
      // Making a column nullable is safe (widening)
      ops.push({
        type: "alter_column",
        table: tableName,
        sql: `ALTER TABLE ${qualifiedTable} ALTER COLUMN "${desired.name}" DROP NOT NULL;`,
        description: `Make ${tableName}.${desired.name} nullable`,
        phase: "structure",
        destructive: false,
      });
    } else {
      // Safe NOT NULL: 4-step pattern
      const groupKey = `safe_not_null_${tableName}_${desired.name}`;
      const checkName = `chk_${tableName}_${desired.name}_nn`;
      const hasExistingCheck = existingCheckMap?.has(checkName);

      if (!hasExistingCheck) {
        // Step 1: ADD CHECK ... NOT VALID
        ops.push({
          type: "add_check_not_valid",
          table: tableName,
          sql: `ALTER TABLE ${qualifiedTable} ADD CONSTRAINT "${checkName}" CHECK ("${desired.name}" IS NOT NULL) NOT VALID;`,
          description: `Add NOT NULL check on ${tableName}.${desired.name} (NOT VALID)`,
          phase: "structure",
          destructive: false,
          group: groupKey,
        });
      }
      // Step 2: VALIDATE CONSTRAINT
      ops.push({
        type: "validate_constraint",
        table: tableName,
        sql: `ALTER TABLE ${qualifiedTable} VALIDATE CONSTRAINT "${checkName}";`,
        description: `Validate NOT NULL check on ${tableName}.${desired.name}`,
        phase: "validate",
        destructive: false,
        group: groupKey,
      });
      // Step 3: SET NOT NULL (instant with validated check constraint, PG 12+)
      ops.push({
        type: "alter_column",
        table: tableName,
        sql: `ALTER TABLE ${qualifiedTable} ALTER COLUMN "${desired.name}" SET NOT NULL;`,
        description: `Make ${tableName}.${desired.name} NOT NULL`,
        phase: "validate",
        destructive: false,
        group: groupKey,
      });
      // Step 4: DROP the helper check constraint (cleanup)
      ops.push({
        type: "alter_column",
        table: tableName,
        sql: `ALTER TABLE ${qualifiedTable} DROP CONSTRAINT "${checkName}";`,
        description: `Drop helper check ${checkName} on ${tableName}`,
        phase: "validate",
        destructive: false,
        group: groupKey,
      });
    }
  }

  // Unique change for existing columns
  if (desired.unique && !existing.unique) {
    // Safe UNIQUE via concurrent index
    const idxName = `idx_${tableName}_${desired.name}_unique`;
    const constraintName = `uq_${tableName}_${desired.name}`;
    ops.push({
      type: "add_unique_index",
      table: tableName,
      sql: `CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "${idxName}" ON "${pgSchema}"."${tableName}" ("${desired.name}");`,
      description: `Create unique index ${idxName} on ${tableName}.${desired.name}`,
      phase: "structure",
      destructive: false,
    });
    ops.push({
      type: "alter_column",
      table: tableName,
      sql: `ALTER TABLE ${qualifiedTable} ADD CONSTRAINT "${constraintName}" UNIQUE USING INDEX "${idxName}";`,
      description: `Add unique constraint ${constraintName} on ${tableName}.${desired.name} using index`,
      phase: "validate",
      destructive: false,
    });
  }

  // Default change
  if (desired.default !== undefined && desired.default !== existing.default) {
    ops.push({
      type: "alter_column",
      table: tableName,
      sql: `ALTER TABLE ${qualifiedTable} ALTER COLUMN "${desired.name}" SET DEFAULT ${desired.default};`,
      description: `Set default for ${tableName}.${desired.name}: ${desired.default}`,
      phase: "structure",
      destructive: false,
    });
  } else if (desired.default === undefined && existing.default !== undefined) {
    ops.push({
      type: "alter_column",
      table: tableName,
      sql: `ALTER TABLE ${qualifiedTable} ALTER COLUMN "${desired.name}" DROP DEFAULT;`,
      description: `Drop default for ${tableName}.${desired.name}`,
      phase: "structure",
      destructive: false,
    });
  }

  return ops;
}

/** Diff CHECK constraints for existing tables (NOT VALID + VALIDATE two-step) */
function planCheckDiff(
  tableName: string,
  desired: CheckDef[],
  existingCheckMap: Map<string, string>,
  pgSchema: string,
): Operation[] {
  const ops: Operation[] = [];
  const qualifiedTable = `"${pgSchema}"."${tableName}"`;

  for (const check of desired) {
    const checkName = check.name || `chk_${tableName}_${Date.now()}`;
    // Skip safe-NOT-NULL helper constraints (handled in planAlterColumn)
    if (checkName.endsWith("_nn")) continue;

    // Check if constraint already exists
    const existingExpr = existingCheckMap.get(checkName);
    if (existingExpr) {
      // Already exists — no action needed (check expression diffing is complex, skip for now)
      continue;
    }

    // New CHECK on existing table: NOT VALID + VALIDATE two-step
    ops.push({
      type: "add_check_not_valid",
      table: tableName,
      sql: `ALTER TABLE ${qualifiedTable} ADD CONSTRAINT "${checkName}" CHECK (${check.expression}) NOT VALID;`,
      description: `Add check constraint ${checkName} on ${tableName} (NOT VALID)`,
      phase: "structure",
      destructive: false,
    });
    ops.push({
      type: "validate_constraint",
      table: tableName,
      sql: `ALTER TABLE ${qualifiedTable} VALIDATE CONSTRAINT "${checkName}";`,
      description: `Validate check constraint ${checkName} on ${tableName}`,
      phase: "validate",
      destructive: false,
    });
  }

  return ops;
}

function planCreateIndex(tableName: string, idx: IndexDef, pgSchema: string): Operation {
  const idxName = idx.name || `idx_${tableName}_${idx.columns.join("_")}`;
  const unique = idx.unique ? "UNIQUE " : "";
  const cols = idx.columns.map((c) => `"${c}"`).join(", ");
  const where = idx.where ? ` WHERE ${idx.where}` : "";

  return {
    type: "add_index",
    table: tableName,
    sql: `CREATE ${unique}INDEX CONCURRENTLY IF NOT EXISTS "${idxName}" ON "${pgSchema}"."${tableName}" (${cols})${where};`,
    description: `Create ${unique ? "unique " : ""}index ${idxName} on ${tableName}`,
    phase: "structure",
    destructive: false,
  };
}

function planCreateTrigger(tableName: string, trigger: TriggerDef, pgSchema: string): Operation {
  const events = trigger.events.join(" OR ");
  const when = trigger.when ? ` WHEN (${trigger.when})` : "";
  const sql = `CREATE TRIGGER "${trigger.name}" ${trigger.timing} ${events} ON "${pgSchema}"."${tableName}" FOR EACH ${trigger.for_each}${when} EXECUTE FUNCTION ${trigger.function}();`;

  return {
    type: "create_trigger",
    table: tableName,
    sql,
    description: `Create trigger ${trigger.name} on ${tableName}`,
    phase: "structure",
    destructive: false,
  };
}

function planDropTrigger(tableName: string, triggerName: string, pgSchema: string, destructive: boolean): Operation {
  return {
    type: "drop_trigger",
    table: tableName,
    sql: `DROP TRIGGER IF EXISTS "${triggerName}" ON "${pgSchema}"."${tableName}";`,
    description: `Drop trigger ${triggerName} on ${tableName}`,
    phase: "structure",
    destructive,
  };
}

function planTriggerDiff(
  tableName: string,
  desired: TriggerDef[],
  current: TriggerDef[],
  pgSchema: string,
): Operation[] {
  const ops: Operation[] = [];
  const currentMap = new Map(current.map((t) => [t.name, t]));
  const desiredMap = new Map(desired.map((t) => [t.name, t]));

  // New or changed triggers
  for (const d of desired) {
    const c = currentMap.get(d.name);
    if (!c) {
      // New trigger
      ops.push(planCreateTrigger(tableName, d, pgSchema));
    } else {
      // Check if changed
      const changed =
        c.timing !== d.timing ||
        c.function !== d.function ||
        c.for_each !== d.for_each ||
        c.when !== d.when ||
        c.events.length !== d.events.length ||
        !c.events.every((e) => d.events.includes(e));

      if (changed) {
        // Drop and recreate — both non-destructive (safe replacement)
        ops.push(planDropTrigger(tableName, d.name, pgSchema, false));
        ops.push(planCreateTrigger(tableName, d, pgSchema));
      }
    }
  }

  // Removed triggers (in DB but not in YAML) → destructive
  for (const c of current) {
    if (!desiredMap.has(c.name)) {
      ops.push(planDropTrigger(tableName, c.name, pgSchema, true));
    }
  }

  return ops;
}

function planEnableRLS(tableName: string, pgSchema: string, force: boolean): Operation[] {
  const ops: Operation[] = [];
  ops.push({
    type: "enable_rls",
    table: tableName,
    sql: `ALTER TABLE "${pgSchema}"."${tableName}" ENABLE ROW LEVEL SECURITY;`,
    description: `Enable RLS on ${tableName}`,
    phase: "structure",
    destructive: false,
  });
  if (force) {
    ops.push({
      type: "enable_rls",
      table: tableName,
      sql: `ALTER TABLE "${pgSchema}"."${tableName}" FORCE ROW LEVEL SECURITY;`,
      description: `Force RLS on ${tableName}`,
      phase: "structure",
      destructive: false,
    });
  }
  return ops;
}

function planDisableRLS(tableName: string, pgSchema: string): Operation[] {
  const ops: Operation[] = [];
  ops.push({
    type: "disable_rls",
    table: tableName,
    sql: `ALTER TABLE "${pgSchema}"."${tableName}" NO FORCE ROW LEVEL SECURITY;`,
    description: `Remove force RLS on ${tableName}`,
    phase: "structure",
    destructive: true,
  });
  ops.push({
    type: "disable_rls",
    table: tableName,
    sql: `ALTER TABLE "${pgSchema}"."${tableName}" DISABLE ROW LEVEL SECURITY;`,
    description: `Disable RLS on ${tableName}`,
    phase: "structure",
    destructive: true,
  });
  return ops;
}

function planCreatePolicy(tableName: string, policy: PolicyDef, pgSchema: string): Operation {
  const permissive = policy.permissive === false ? "RESTRICTIVE" : "PERMISSIVE";
  const forClause = policy.for;
  const toClause = policy.to && policy.to.length > 0 ? policy.to.join(", ") : "PUBLIC";
  let sql = `CREATE POLICY "${policy.name}" ON "${pgSchema}"."${tableName}" AS ${permissive} FOR ${forClause} TO ${toClause}`;
  if (policy.using) {
    sql += ` USING (${policy.using})`;
  }
  if (policy.check) {
    sql += ` WITH CHECK (${policy.check})`;
  }
  sql += ";";

  return {
    type: "create_policy",
    table: tableName,
    sql,
    description: `Create policy ${policy.name} on ${tableName}`,
    phase: "structure",
    destructive: false,
  };
}

function planDropPolicy(tableName: string, policyName: string, pgSchema: string, destructive: boolean): Operation {
  return {
    type: "drop_policy",
    table: tableName,
    sql: `DROP POLICY IF EXISTS "${policyName}" ON "${pgSchema}"."${tableName}";`,
    description: `Drop policy ${policyName} on ${tableName}`,
    phase: "structure",
    destructive,
  };
}

function planPolicyDiff(
  tableName: string,
  desired: PolicyDef[],
  current: PolicyDef[],
  pgSchema: string,
): Operation[] {
  const ops: Operation[] = [];
  const currentMap = new Map(current.map((p) => [p.name, p]));
  const desiredMap = new Map(desired.map((p) => [p.name, p]));

  // New or changed policies
  for (const d of desired) {
    const c = currentMap.get(d.name);
    if (!c) {
      // New policy
      ops.push(planCreatePolicy(tableName, d, pgSchema));
    } else {
      // Check if changed
      const currentPermissive = c.permissive !== false;
      const desiredPermissive = d.permissive !== false;
      const changed =
        c.for !== d.for ||
        (c.using || "") !== (d.using || "") ||
        (c.check || "") !== (d.check || "") ||
        currentPermissive !== desiredPermissive ||
        JSON.stringify(c.to || []) !== JSON.stringify(d.to || []);

      if (changed) {
        // Drop and recreate — both non-destructive (safe replacement)
        ops.push(planDropPolicy(tableName, d.name, pgSchema, false));
        ops.push(planCreatePolicy(tableName, d, pgSchema));
      }
    }
  }

  // Removed policies (in DB but not in YAML) → destructive
  for (const c of current) {
    if (!desiredMap.has(c.name)) {
      ops.push(planDropPolicy(tableName, c.name, pgSchema, true));
    }
  }

  return ops;
}

function planRLSDiff(
  tableName: string,
  desired: { rls?: boolean; force_rls?: boolean },
  current: { rls: boolean; force_rls: boolean },
  pgSchema: string,
): Operation[] {
  const ops: Operation[] = [];
  const desiredRls = desired.rls === true;
  const desiredForce = desired.force_rls === true;

  if (desiredRls && !current.rls) {
    // Enable RLS
    ops.push({
      type: "enable_rls",
      table: tableName,
      sql: `ALTER TABLE "${pgSchema}"."${tableName}" ENABLE ROW LEVEL SECURITY;`,
      description: `Enable RLS on ${tableName}`,
      phase: "structure",
      destructive: false,
    });
  } else if (!desiredRls && current.rls) {
    // Disable RLS — destructive
    ops.push({
      type: "disable_rls",
      table: tableName,
      sql: `ALTER TABLE "${pgSchema}"."${tableName}" DISABLE ROW LEVEL SECURITY;`,
      description: `Disable RLS on ${tableName}`,
      phase: "structure",
      destructive: true,
    });
  }

  if (desiredForce && !current.force_rls) {
    // Force RLS — non-destructive
    ops.push({
      type: "enable_rls",
      table: tableName,
      sql: `ALTER TABLE "${pgSchema}"."${tableName}" FORCE ROW LEVEL SECURITY;`,
      description: `Force RLS on ${tableName}`,
      phase: "structure",
      destructive: false,
    });
  } else if (!desiredForce && current.force_rls) {
    // Remove force RLS — destructive
    ops.push({
      type: "disable_rls",
      table: tableName,
      sql: `ALTER TABLE "${pgSchema}"."${tableName}" NO FORCE ROW LEVEL SECURITY;`,
      description: `Remove force RLS on ${tableName}`,
      phase: "structure",
      destructive: true,
    });
  }

  return ops;
}

function buildAddColumnSql(tableName: string, col: ColumnDef, pgSchema: string): string {
  let sql = `ALTER TABLE "${pgSchema}"."${tableName}" ADD COLUMN "${col.name}" ${col.type}`;
  if (col.nullable === false) sql += " NOT NULL";
  if (col.unique) sql += " UNIQUE";
  if (col.default !== undefined) sql += ` DEFAULT ${col.default}`;
  sql += ";";
  return sql;
}

/**
 * Determine if a type change is narrowing (potential data loss).
 * Widening changes (int→bigint, varchar(50)→varchar(255)) are safe.
 * Narrowing changes (bigint→int, varchar(255)→varchar(50), text→integer) are destructive.
 */
function isNarrowingTypeChange(from: string, to: string): boolean {
  const fromNorm = normalizeType(from).toLowerCase();
  const toNorm = normalizeType(to).toLowerCase();

  // Same normalized type → check size parameters
  if (fromNorm === toNorm) return false;

  // varchar(N) → varchar(M): narrowing if M < N
  const fromVarchar = fromNorm.match(/^varchar\((\d+)\)$/);
  const toVarchar = toNorm.match(/^varchar\((\d+)\)$/);
  if (fromVarchar && toVarchar) {
    return parseInt(toVarchar[1]) < parseInt(fromVarchar[1]);
  }

  // Known safe widenings
  const widenings: [string, string][] = [
    ["smallint", "integer"],
    ["smallint", "bigint"],
    ["integer", "bigint"],
    ["real", "double precision"],
    ["varchar", "text"],
  ];

  for (const [narrow, wide] of widenings) {
    // Also handle varchar(N) → text
    if ((fromNorm === narrow || fromNorm.startsWith(`${narrow}(`)) && toNorm === wide) return false;
    if (fromNorm === wide && (toNorm === narrow || toNorm.startsWith(`${narrow}(`))) return true;
  }

  // Any other type change is considered narrowing (conservative)
  return true;
}

export function normalizeType(t: string): string {
  const lower = t.toLowerCase().trim();
  const map: Record<string, string> = {
    serial: "integer",
    bigserial: "bigint",
    smallserial: "smallint",
    int: "integer",
    int4: "integer",
    int8: "bigint",
    int2: "smallint",
    float4: "real",
    float8: "double precision",
    bool: "boolean",
    timestamptz: "timestamp with time zone",
    "timestamp with time zone": "timestamptz",
  };
  return map[lower] || lower;
}

function isSerialType(t: string): boolean {
  return ["serial", "bigserial", "smallserial"].includes(t.toLowerCase());
}

function isIntegerType(t: string): boolean {
  return ["integer", "bigint", "smallint", "int", "int4", "int8", "int2"].includes(t.toLowerCase());
}
