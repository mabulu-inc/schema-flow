// src/planner/index.ts
// Diff engine: compare desired YAML state with current DB state, produce SQL operations
// Key design: CREATE TABLEs first (without FKs), then ALTER TABLEs for FKs last

import type { TableSchema, ColumnDef, ForeignKeyAction, IndexDef, CheckDef } from "../schema/types.js";
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
  | "create_function";

export interface Operation {
  type: OperationType;
  table?: string;
  sql: string;
  description: string;
  phase: "structure" | "foreign_key";
}

export interface MigrationPlan {
  operations: Operation[];
  structureOps: Operation[];
  foreignKeyOps: Operation[];
  summary: {
    tablesToCreate: string[];
    tablesToAlter: string[];
    foreignKeysToAdd: number;
    totalOperations: number;
  };
}

export async function buildPlan(
  client: pg.PoolClient,
  desiredSchemas: TableSchema[],
  pgSchema: string
): Promise<MigrationPlan> {
  const existingTables = new Set(await getExistingTables(client, pgSchema));
  const operations: Operation[] = [];

  for (const desired of desiredSchemas) {
    if (!existingTables.has(desired.table)) {
      // CREATE TABLE (without foreign keys — those come later)
      const createOps = planCreateTable(desired, pgSchema);
      operations.push(...createOps);
    } else {
      // ALTER TABLE — diff columns, indexes, checks
      const alterOps = await planAlterTable(client, desired, pgSchema);
      operations.push(...alterOps);
    }
  }

  // Separate structure ops from FK ops
  const structureOps = operations.filter((op) => op.phase === "structure");
  const foreignKeyOps = operations.filter((op) => op.phase === "foreign_key");

  const tablesToCreate = [...new Set(
    operations.filter((o) => o.type === "create_table").map((o) => o.table!)
  )];

  const tablesToAlter = [...new Set(
    operations
      .filter((o) => o.type !== "create_table" && o.table)
      .map((o) => o.table!)
      .filter((t) => !tablesToCreate.includes(t))
  )];

  return {
    operations: [...structureOps, ...foreignKeyOps],
    structureOps,
    foreignKeyOps,
    summary: {
      tablesToCreate,
      tablesToAlter,
      foreignKeysToAdd: foreignKeyOps.length,
      totalOperations: structureOps.length + foreignKeyOps.length,
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
      });
    }
  }

  // FK ops come last
  ops.push(...fkOps);

  return ops;
}

async function planAlterTable(
  client: pg.PoolClient,
  desired: TableSchema,
  pgSchema: string
): Promise<Operation[]> {
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
        });
      }

      continue;
    }

    // Alter existing columns if type/nullable/default changed
    const existing = currentColMap.get(col.name)!;
    const alterOps = planAlterColumn(desired.table, existing, col, pgSchema);
    ops.push(...alterOps);
  }

  // Drop columns that are no longer in the schema
  for (const existingCol of current.columns) {
    if (!desiredColMap.has(existingCol.name)) {
      ops.push({
        type: "drop_column",
        table: desired.table,
        sql: `ALTER TABLE "${pgSchema}"."${desired.table}" DROP COLUMN "${existingCol.name}";`,
        description: `Drop column ${desired.table}.${existingCol.name}`,
        phase: "structure",
      });
    }
  }

  return ops;
}

function planAlterColumn(
  tableName: string,
  existing: ColumnDef,
  desired: ColumnDef,
  pgSchema: string
): Operation[] {
  const ops: Operation[] = [];
  const qualifiedTable = `"${pgSchema}"."${tableName}"`;

  // Type change
  if (normalizeType(existing.type) !== normalizeType(desired.type)) {
    // Skip type change for serial types (they're just integer + sequence)
    if (!isSerialType(desired.type) || !isIntegerType(existing.type)) {
      ops.push({
        type: "alter_column",
        table: tableName,
        sql: `ALTER TABLE ${qualifiedTable} ALTER COLUMN "${desired.name}" TYPE ${desired.type} USING "${desired.name}"::${desired.type};`,
        description: `Change type of ${tableName}.${desired.name}: ${existing.type} → ${desired.type}`,
        phase: "structure",
      });
    }
  }

  // Nullable change
  const existingNullable = existing.nullable !== false;
  const desiredNullable = desired.nullable === true;
  if (existingNullable !== desiredNullable && !desired.primary_key) {
    if (desiredNullable) {
      ops.push({
        type: "alter_column",
        table: tableName,
        sql: `ALTER TABLE ${qualifiedTable} ALTER COLUMN "${desired.name}" DROP NOT NULL;`,
        description: `Make ${tableName}.${desired.name} nullable`,
        phase: "structure",
      });
    } else {
      ops.push({
        type: "alter_column",
        table: tableName,
        sql: `ALTER TABLE ${qualifiedTable} ALTER COLUMN "${desired.name}" SET NOT NULL;`,
        description: `Make ${tableName}.${desired.name} NOT NULL`,
        phase: "structure",
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
    });
  } else if (desired.default === undefined && existing.default !== undefined) {
    ops.push({
      type: "alter_column",
      table: tableName,
      sql: `ALTER TABLE ${qualifiedTable} ALTER COLUMN "${desired.name}" DROP DEFAULT;`,
      description: `Drop default for ${tableName}.${desired.name}`,
      phase: "structure",
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
  };
}

function buildAddColumnSql(tableName: string, col: ColumnDef, pgSchema: string): string {
  let sql = `ALTER TABLE "${pgSchema}"."${tableName}" ADD COLUMN "${col.name}" ${col.type}`;
  if (col.nullable === false) sql += " NOT NULL";
  if (col.unique) sql += " UNIQUE";
  if (col.default !== undefined) sql += ` DEFAULT ${col.default}`;
  sql += ";";
  return sql;
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
