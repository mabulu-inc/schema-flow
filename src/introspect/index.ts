// src/introspect/index.ts
// Introspect the current PostgreSQL database state

import pg from "pg";
import type { TableSchema, ColumnDef, TriggerDef, PolicyDef } from "../schema/types.js";

export interface DbColumn {
  column_name: string;
  data_type: string;
  udt_name: string;
  is_nullable: string;
  column_default: string | null;
  character_maximum_length: number | null;
  numeric_precision: number | null;
  numeric_scale: number | null;
}

export interface DbConstraint {
  constraint_name: string;
  constraint_type: string;
  column_name: string;
  foreign_table_name: string | null;
  foreign_column_name: string | null;
  delete_rule: string | null;
  update_rule: string | null;
}

export interface DbIndex {
  indexname: string;
  indexdef: string;
}

export interface DbFunction {
  routine_name: string;
  routine_type: string;
  data_type: string;
  external_language: string;
  routine_definition: string;
  parameter_list: string;
  security_type: string;
}

export interface DbTrigger {
  trigger_name: string;
  event_manipulation: string;
  action_timing: string;
  action_orientation: string;
  action_condition: string | null;
  function_name: string;
}

/** Get triggers for a table */
export async function getTableTriggers(
  client: pg.PoolClient,
  tableName: string,
  pgSchema: string,
): Promise<TriggerDef[]> {
  const res = await client.query<DbTrigger>(
    `SELECT
       t.trigger_name,
       t.event_manipulation,
       t.action_timing,
       t.action_orientation,
       t.action_condition,
       p.proname AS function_name
     FROM information_schema.triggers t
     JOIN pg_trigger pg_t ON pg_t.tgname = t.trigger_name
     JOIN pg_proc p ON pg_t.tgfoid = p.oid
     JOIN pg_class c ON pg_t.tgrelid = c.oid
     JOIN pg_namespace n ON c.relnamespace = n.oid
     WHERE t.trigger_schema = $1
       AND t.event_object_table = $2
       AND NOT pg_t.tgisinternal
     ORDER BY t.trigger_name, t.event_manipulation`,
    [pgSchema, tableName],
  );

  return dbTriggersToTriggerDefs(res.rows);
}

/** Group raw trigger rows (one per event) into TriggerDef objects */
export function dbTriggersToTriggerDefs(rows: DbTrigger[]): TriggerDef[] {
  const grouped = new Map<string, DbTrigger[]>();
  for (const row of rows) {
    if (!grouped.has(row.trigger_name)) {
      grouped.set(row.trigger_name, []);
    }
    grouped.get(row.trigger_name)!.push(row);
  }

  const triggers: TriggerDef[] = [];
  for (const [name, triggerRows] of grouped) {
    const first = triggerRows[0];
    triggers.push({
      name,
      timing: first.action_timing as TriggerDef["timing"],
      events: triggerRows.map((r) => r.event_manipulation as TriggerDef["events"][number]),
      function: first.function_name,
      for_each: first.action_orientation === "ROW" ? "ROW" : "STATEMENT",
      when: first.action_condition || undefined,
    });
  }

  return triggers;
}

export interface DbPolicy {
  policyname: string;
  cmd: string;
  permissive: string;
  roles: string[] | string;
  qual: string | null;
  with_check: string | null;
}

/** Get RLS policies for a table */
export async function getTablePolicies(
  client: pg.PoolClient,
  tableName: string,
  pgSchema: string,
): Promise<PolicyDef[]> {
  const res = await client.query<DbPolicy>(
    `SELECT
       policyname,
       cmd,
       permissive,
       roles,
       qual,
       with_check
     FROM pg_policies
     WHERE schemaname = $1 AND tablename = $2
     ORDER BY policyname`,
    [pgSchema, tableName],
  );

  return res.rows.map((row) => {
    const policy: PolicyDef = {
      name: row.policyname,
      for: row.cmd as PolicyDef["for"],
      permissive: row.permissive === "PERMISSIVE",
    };

    // Map roles — pg_policies returns name[] which pg may deliver as string[] or a raw string
    let roles: string[];
    if (Array.isArray(row.roles)) {
      roles = row.roles;
    } else if (typeof row.roles === "string") {
      // Parse PostgreSQL array literal like "{role1,role2}"
      roles = row.roles.replace(/^\{|\}$/g, "").split(",").filter(Boolean);
    } else {
      roles = [];
    }
    if (roles.length > 0) {
      const filtered = roles.filter((r) => r.toLowerCase() !== "public");
      if (filtered.length > 0) {
        policy.to = filtered;
      }
    }

    if (row.qual) {
      policy.using = row.qual;
    }
    if (row.with_check) {
      policy.check = row.with_check;
    }

    return policy;
  });
}

/** Get RLS status for a table */
export async function getTableRLS(
  client: pg.PoolClient,
  tableName: string,
  pgSchema: string,
): Promise<{ rls: boolean; force_rls: boolean }> {
  const res = await client.query(
    `SELECT c.relrowsecurity, c.relforcerowsecurity
     FROM pg_class c
     JOIN pg_namespace n ON c.relnamespace = n.oid
     WHERE n.nspname = $1 AND c.relname = $2`,
    [pgSchema, tableName],
  );

  if (res.rows.length === 0) {
    return { rls: false, force_rls: false };
  }

  return {
    rls: res.rows[0].relrowsecurity,
    force_rls: res.rows[0].relforcerowsecurity,
  };
}

/** Get all user tables in the schema */
export async function getExistingTables(client: pg.PoolClient, pgSchema: string): Promise<string[]> {
  const res = await client.query(
    `SELECT table_name FROM information_schema.tables
     WHERE table_schema = $1 AND table_type = 'BASE TABLE'
     ORDER BY table_name`,
    [pgSchema],
  );
  return res.rows.map((r) => r.table_name);
}

/** Get columns for a specific table */
export async function getTableColumns(client: pg.PoolClient, tableName: string, pgSchema: string): Promise<DbColumn[]> {
  const res = await client.query<DbColumn>(
    `SELECT
       column_name,
       data_type,
       udt_name,
       is_nullable,
       column_default,
       character_maximum_length,
       numeric_precision,
       numeric_scale
     FROM information_schema.columns
     WHERE table_schema = $1 AND table_name = $2
     ORDER BY ordinal_position`,
    [pgSchema, tableName],
  );
  return res.rows;
}

/** Get all constraints for a table using pg_catalog (works regardless of ownership) */
export async function getTableConstraints(
  client: pg.PoolClient,
  tableName: string,
  pgSchema: string,
): Promise<DbConstraint[]> {
  const res = await client.query<DbConstraint>(
    `SELECT
       c.conname AS constraint_name,
       CASE c.contype
         WHEN 'p' THEN 'PRIMARY KEY'
         WHEN 'f' THEN 'FOREIGN KEY'
         WHEN 'u' THEN 'UNIQUE'
         WHEN 'c' THEN 'CHECK'
       END AS constraint_type,
       a.attname AS column_name,
       ft.relname AS foreign_table_name,
       fa.attname AS foreign_column_name,
       CASE c.confdeltype
         WHEN 'a' THEN 'NO ACTION'
         WHEN 'r' THEN 'RESTRICT'
         WHEN 'c' THEN 'CASCADE'
         WHEN 'n' THEN 'SET NULL'
         WHEN 'd' THEN 'SET DEFAULT'
       END AS delete_rule,
       CASE c.confupdtype
         WHEN 'a' THEN 'NO ACTION'
         WHEN 'r' THEN 'RESTRICT'
         WHEN 'c' THEN 'CASCADE'
         WHEN 'n' THEN 'SET NULL'
         WHEN 'd' THEN 'SET DEFAULT'
       END AS update_rule
     FROM pg_constraint c
     JOIN pg_class t ON c.conrelid = t.oid
     JOIN pg_namespace n ON t.relnamespace = n.oid
     LEFT JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS ak(attnum, ord) ON true
     LEFT JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ak.attnum
     LEFT JOIN pg_class ft ON c.confrelid = ft.oid
     LEFT JOIN LATERAL unnest(c.confkey) WITH ORDINALITY AS fk(attnum, ord) ON fk.ord = ak.ord
     LEFT JOIN pg_attribute fa ON fa.attrelid = ft.oid AND fa.attnum = fk.attnum
     WHERE n.nspname = $1 AND t.relname = $2
     ORDER BY c.conname, ak.ord`,
    [pgSchema, tableName],
  );
  return res.rows;
}

/** Get indexes for a table */
export async function getTableIndexes(client: pg.PoolClient, tableName: string, pgSchema: string): Promise<DbIndex[]> {
  const res = await client.query<DbIndex>(
    `SELECT indexname, indexdef
     FROM pg_indexes
     WHERE schemaname = $1 AND tablename = $2`,
    [pgSchema, tableName],
  );
  return res.rows;
}

/** Build a full picture of the current DB state for a single table */
export async function introspectTable(
  client: pg.PoolClient,
  tableName: string,
  pgSchema: string,
): Promise<TableSchema> {
  const dbCols = await getTableColumns(client, tableName, pgSchema);
  const dbConstraints = await getTableConstraints(client, tableName, pgSchema);

  // Group constraints
  const pkColumns: string[] = [];
  const uniqueColumns = new Set<string>();
  const fkMap = new Map<string, DbConstraint[]>();

  for (const c of dbConstraints) {
    if (c.constraint_type === "PRIMARY KEY") {
      pkColumns.push(c.column_name);
    } else if (c.constraint_type === "UNIQUE") {
      uniqueColumns.add(c.column_name);
    } else if (c.constraint_type === "FOREIGN KEY") {
      if (!fkMap.has(c.constraint_name)) fkMap.set(c.constraint_name, []);
      fkMap.get(c.constraint_name)!.push(c);
    }
  }

  const isSinglePk = pkColumns.length === 1;

  const columns: ColumnDef[] = dbCols.map((col) => {
    const colType = resolveColumnType(col);
    const def: ColumnDef = {
      name: col.column_name,
      type: colType,
    };

    if (col.is_nullable === "YES") {
      def.nullable = true;
    }

    if (col.column_default !== null && !col.column_default.startsWith("nextval(")) {
      def.default = col.column_default;
    }

    if (isSinglePk && pkColumns[0] === col.column_name) {
      def.primary_key = true;
    }

    if (uniqueColumns.has(col.column_name)) {
      def.unique = true;
    }

    // Check for FK on this column (single-column FK)
    for (const [, fkCols] of fkMap) {
      if (fkCols.length === 1 && fkCols[0].column_name === col.column_name) {
        def.references = {
          table: fkCols[0].foreign_table_name!,
          column: fkCols[0].foreign_column_name!,
          on_delete: (fkCols[0].delete_rule || "NO ACTION") as ColumnDef["references"] extends { on_delete?: infer T }
            ? T
            : never,
          on_update: (fkCols[0].update_rule || "NO ACTION") as ColumnDef["references"] extends { on_update?: infer T }
            ? T
            : never,
        };
      }
    }

    return def;
  });

  const schema: TableSchema = {
    table: tableName,
    columns,
  };

  if (!isSinglePk && pkColumns.length > 1) {
    schema.primary_key = pkColumns;
  }

  // Introspect triggers
  const triggers = await getTableTriggers(client, tableName, pgSchema);
  if (triggers.length > 0) {
    schema.triggers = triggers;
  }

  // Introspect RLS
  const rlsState = await getTableRLS(client, tableName, pgSchema);
  if (rlsState.rls) {
    schema.rls = true;
  }
  if (rlsState.force_rls) {
    schema.force_rls = true;
  }

  // Introspect policies
  const policies = await getTablePolicies(client, tableName, pgSchema);
  if (policies.length > 0) {
    schema.policies = policies;
  }

  return schema;
}

/** Get all functions in the schema */
export async function getExistingFunctions(client: pg.PoolClient, pgSchema: string): Promise<DbFunction[]> {
  const res = await client.query<DbFunction>(
    `SELECT
       r.routine_name,
       r.routine_type,
       r.data_type,
       r.external_language,
       r.routine_definition,
       r.security_type,
       COALESCE(
         string_agg(p.parameter_name || ' ' || p.data_type, ', ' ORDER BY p.ordinal_position),
         ''
       ) AS parameter_list
     FROM information_schema.routines r
     LEFT JOIN information_schema.parameters p
       ON r.specific_name = p.specific_name AND p.parameter_mode = 'IN'
     WHERE r.routine_schema = $1
       AND r.routine_type = 'FUNCTION'
       AND r.routine_name NOT LIKE 'pg_%'
     GROUP BY r.routine_name, r.routine_type, r.data_type, r.external_language, r.routine_definition, r.security_type
     ORDER BY r.routine_name`,
    [pgSchema],
  );
  return res.rows;
}

/** Resolve a column type from information_schema to a user-friendly string */
function resolveColumnType(col: DbColumn): string {
  const { udt_name, character_maximum_length, numeric_precision, numeric_scale } = col;

  // Handle serial types via column_default
  if (col.column_default?.startsWith("nextval(")) {
    if (udt_name === "int4") return "serial";
    if (udt_name === "int8") return "bigserial";
    if (udt_name === "int2") return "smallserial";
  }

  // Handle common type mappings
  switch (udt_name) {
    case "int4":
      return "integer";
    case "int8":
      return "bigint";
    case "int2":
      return "smallint";
    case "float4":
      return "real";
    case "float8":
      return "double precision";
    case "bool":
      return "boolean";
    case "varchar":
      return character_maximum_length ? `varchar(${character_maximum_length})` : "varchar";
    case "bpchar":
      return character_maximum_length ? `char(${character_maximum_length})` : "char";
    case "numeric":
      if (numeric_precision && numeric_scale) return `numeric(${numeric_precision},${numeric_scale})`;
      if (numeric_precision) return `numeric(${numeric_precision})`;
      return "numeric";
    case "timestamptz":
      return "timestamptz";
    case "timestamp":
      return "timestamp";
    default:
      return udt_name;
  }
}
