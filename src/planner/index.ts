// src/planner/index.ts
// Diff engine: compare desired YAML state with current DB state, produce SQL operations
// Key design: CREATE TABLEs first (without FKs), then ALTER TABLEs for FKs last
// Safety: destructive operations (drops, narrowing changes) are blocked by default

import type { TableSchema, ColumnDef, IndexDef, TriggerDef } from "../schema/types.js";
import { introspectTable, getExistingTables } from "../introspect/index.js";
import { logger } from "../core/logger.js";
import pg from "pg";

export type OperationType =
  | "create_table"
  | "add_column"
  | "alter_column"
  | "drop_column"
  | "add_index"
  | "drop_index"
  | "add_check"
  | "add_foreign_key"
  | "drop_foreign_key"
  | "drop_table"
  | "create_function"
  | "create_trigger"
  | "drop_trigger";

export interface Operation {
  type: OperationType;
  table?: string;
  sql: string;
  description: string;
  phase: "structure" | "foreign_key";
  /** Whether this operation is destructive (data loss, breaking change) */
  destructive: boolean;
}

export interface MigrationPlan {
  operations: Operation[];
  structureOps: Operation[];
  foreignKeyOps: Operation[];
  /** Destructive operations that were blocked (safe mode) */
  blocked: Operation[];
  summary: {
    tablesToCreate: string[];
    tablesToAlter: string[];
    foreignKeysToAdd: number;
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

  // Separate structure ops from FK ops (only from allowed)
  const structureOps = allowed.filter((op) => op.phase === "structure");
  const foreignKeyOps = allowed.filter((op) => op.phase === "foreign_key");

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
    operations: [...structureOps, ...foreignKeyOps],
    structureOps,
    foreignKeyOps,
    blocked,
    summary: {
      tablesToCreate,
      tablesToAlter,
      foreignKeysToAdd: foreignKeyOps.length,
      totalOperations: structureOps.length + foreignKeyOps.length,
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

    // Collect foreign keys for deferred application
    if (col.references) {
      const fkName = `fk_${schema.table}_${col.name}_${col.references.table}`;
      const onDelete = col.references.on_delete || "NO ACTION";
      const onUpdate = col.references.on_update || "NO ACTION";
      fkOps.push({
        type: "add_foreign_key",
        table: schema.table,
        sql: `ALTER TABLE "${pgSchema}"."${schema.table}" ADD CONSTRAINT "${fkName}" FOREIGN KEY ("${col.name}") REFERENCES "${pgSchema}"."${col.references.table}" ("${col.references.column}") ON DELETE ${onDelete} ON UPDATE ${onUpdate};`,
        description: `Add FK ${schema.table}.${col.name} → ${col.references.table}.${col.references.column}`,
        phase: "foreign_key",
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

  // FK ops come last
  ops.push(...fkOps);

  return ops;
}

async function planAlterTable(client: pg.PoolClient, desired: TableSchema, pgSchema: string): Promise<Operation[]> {
  const ops: Operation[] = [];
  const current = await introspectTable(client, desired.table, pgSchema);

  const currentColMap = new Map(current.columns.map((c) => [c.name, c]));
  const desiredColMap = new Map(desired.columns.map((c) => [c.name, c]));

  // Add new columns
  for (const col of desired.columns) {
    if (!currentColMap.has(col.name)) {
      const addSql = buildAddColumnSql(desired.table, col, pgSchema);
      ops.push({
        type: "add_column",
        table: desired.table,
        sql: addSql,
        description: `Add column ${desired.table}.${col.name}`,
        phase: "structure",
        destructive: false,
      });

      // FK for new column
      if (col.references) {
        const fkName = `fk_${desired.table}_${col.name}_${col.references.table}`;
        const onDelete = col.references.on_delete || "NO ACTION";
        const onUpdate = col.references.on_update || "NO ACTION";
        ops.push({
          type: "add_foreign_key",
          table: desired.table,
          sql: `ALTER TABLE "${pgSchema}"."${desired.table}" ADD CONSTRAINT "${fkName}" FOREIGN KEY ("${col.name}") REFERENCES "${pgSchema}"."${col.references.table}" ("${col.references.column}") ON DELETE ${onDelete} ON UPDATE ${onUpdate};`,
          description: `Add FK ${desired.table}.${col.name} → ${col.references.table}.${col.references.column}`,
          phase: "foreign_key",
          destructive: false,
        });
      }

      continue;
    }

    // Alter existing columns if type/nullable/default changed
    const existing = currentColMap.get(col.name)!;
    const alterOps = planAlterColumn(desired.table, existing, col, pgSchema);
    ops.push(...alterOps);
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

  // Trigger diff
  const triggerOps = planTriggerDiff(desired.table, desired.triggers || [], current.triggers || [], pgSchema);
  ops.push(...triggerOps);

  return ops;
}

function planAlterColumn(tableName: string, existing: ColumnDef, desired: ColumnDef, pgSchema: string): Operation[] {
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

  // Nullable change
  // Convention: columns are NOT NULL by default.
  // nullable=true means nullable; nullable=undefined or nullable=false means NOT NULL.
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
      // Making a column NOT NULL is destructive (can fail if NULLs exist)
      ops.push({
        type: "alter_column",
        table: tableName,
        sql: `ALTER TABLE ${qualifiedTable} ALTER COLUMN "${desired.name}" SET NOT NULL;`,
        description: `Make ${tableName}.${desired.name} NOT NULL`,
        phase: "structure",
        destructive: true,
      });
    }
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

function normalizeType(t: string): string {
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
