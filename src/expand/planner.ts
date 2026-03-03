// src/expand/planner.ts
// Plan expand and contract operations

import type { Operation } from "../planner/index.js";
import type { ExpandDef } from "../schema/types.js";

/** Plan the expand phase for a column with an expand definition */
export function planExpandColumn(
  tableName: string,
  newColumnName: string,
  columnType: string,
  expand: ExpandDef,
  pgSchema: string,
): Operation[] {
  const ops: Operation[] = [];
  const qualifiedTable = `"${pgSchema}"."${tableName}"`;
  const triggerName = `_sf_dw_${tableName}_${expand.from}_${newColumnName}`;
  const functionName = `_sf_dw_${tableName}_${expand.from}_${newColumnName}`;
  const batchSize = expand.batch_size || 1000;

  // Step 1: ADD COLUMN (nullable)
  ops.push({
    type: "expand_column",
    table: tableName,
    sql: `ALTER TABLE ${qualifiedTable} ADD COLUMN IF NOT EXISTS "${newColumnName}" ${columnType};`,
    description: `Add column ${tableName}.${newColumnName} (expand from ${expand.from})`,
    phase: "structure",
    destructive: false,
    meta: { expand: true, from: expand.from, to: newColumnName },
  });

  // Step 2: Create dual-write trigger function
  const reverseClause = expand.reverse
    ? `NEW."${expand.from}" = ${expand.reverse};`
    : "";
  const fnBody = `
BEGIN
  IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
    NEW."${newColumnName}" = ${expand.transform};
    ${reverseClause}
  END IF;
  RETURN NEW;
END;
`.trim();

  ops.push({
    type: "create_dual_write_trigger",
    table: tableName,
    sql: `CREATE OR REPLACE FUNCTION "${functionName}"() RETURNS trigger LANGUAGE plpgsql AS $fn_body$\n${fnBody}\n$fn_body$;`,
    description: `Create dual-write function ${functionName}`,
    phase: "structure",
    destructive: false,
    meta: { expand: true },
  });

  // Step 3: Create the trigger
  ops.push({
    type: "create_dual_write_trigger",
    table: tableName,
    sql: `CREATE TRIGGER "${triggerName}" BEFORE INSERT OR UPDATE ON ${qualifiedTable} FOR EACH ROW EXECUTE FUNCTION "${functionName}"();`,
    description: `Create dual-write trigger ${triggerName} on ${tableName}`,
    phase: "structure",
    destructive: false,
    meta: { expand: true },
  });

  // Step 4: Backfill (batched — handled at execution time, not as SQL)
  ops.push({
    type: "backfill_column",
    table: tableName,
    sql: `-- Backfill ${tableName}.${newColumnName} = ${expand.transform} WHERE "${newColumnName}" IS NULL (batches of ${batchSize})`,
    description: `Backfill ${tableName}.${newColumnName} from ${expand.from} (batch_size=${batchSize})`,
    phase: "structure",
    destructive: false,
    meta: {
      expand: true,
      backfill: true,
      from: expand.from,
      to: newColumnName,
      transform: expand.transform,
      batchSize,
      triggerName,
      functionName,
    },
  });

  return ops;
}

/** Plan the contract phase (cleanup after expand) */
export function planContractColumn(
  tableName: string,
  oldColumn: string,
  newColumn: string,
  triggerName: string,
  functionName: string,
  pgSchema: string,
): Operation[] {
  const ops: Operation[] = [];
  const qualifiedTable = `"${pgSchema}"."${tableName}"`;

  // Drop the dual-write trigger
  ops.push({
    type: "drop_dual_write_trigger",
    table: tableName,
    sql: `DROP TRIGGER IF EXISTS "${triggerName}" ON ${qualifiedTable};`,
    description: `Drop dual-write trigger ${triggerName} on ${tableName}`,
    phase: "structure",
    destructive: false,
    meta: { contract: true },
  });

  // Drop the trigger function
  ops.push({
    type: "drop_dual_write_trigger",
    table: tableName,
    sql: `DROP FUNCTION IF EXISTS "${functionName}"();`,
    description: `Drop dual-write function ${functionName}`,
    phase: "structure",
    destructive: false,
    meta: { contract: true },
  });

  // Drop the old column
  ops.push({
    type: "contract_column",
    table: tableName,
    sql: `ALTER TABLE ${qualifiedTable} DROP COLUMN IF EXISTS "${oldColumn}";`,
    description: `Drop old column ${tableName}.${oldColumn} (contracted)`,
    phase: "structure",
    destructive: true,
    meta: { contract: true, oldColumn, newColumn },
  });

  return ops;
}
