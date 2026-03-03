// src/introspect/index.ts
// Introspect the current PostgreSQL database state

import pg from "pg";
import type { TableSchema, ColumnDef, TriggerDef, PolicyDef, EnumSchema, ViewSchema, MaterializedViewSchema, IndexDef } from "../schema/types.js";

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
  convalidated: boolean;
  check_expression: string | null;
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
       END AS update_rule,
       c.convalidated,
       pg_get_constraintdef(c.oid) AS check_expression
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
          validated: fkCols[0].convalidated,
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

// ─── Enum Introspection ──────────────────────────────────────────────────────

export interface DbEnum {
  typname: string;
  enumlabel: string;
  enumsortorder: number;
}

/** Get all user-defined enums in the schema */
export async function getExistingEnums(client: pg.PoolClient, pgSchema: string): Promise<EnumSchema[]> {
  const res = await client.query<DbEnum>(
    `SELECT t.typname, e.enumlabel, e.enumsortorder
     FROM pg_type t
     JOIN pg_enum e ON t.oid = e.enumtypid
     JOIN pg_namespace n ON t.typnamespace = n.oid
     WHERE n.nspname = $1
     ORDER BY t.typname, e.enumsortorder`,
    [pgSchema],
  );

  const enumMap = new Map<string, string[]>();
  for (const row of res.rows) {
    if (!enumMap.has(row.typname)) enumMap.set(row.typname, []);
    enumMap.get(row.typname)!.push(row.enumlabel);
  }

  return Array.from(enumMap.entries()).map(([name, values]) => ({ name, values }));
}

// ─── Extension Introspection ─────────────────────────────────────────────────

/** Get all installed extensions */
export async function getExistingExtensions(client: pg.PoolClient): Promise<string[]> {
  const res = await client.query(
    `SELECT extname FROM pg_extension WHERE extname <> 'plpgsql' ORDER BY extname`,
  );
  return res.rows.map((r) => r.extname);
}

// ─── View Introspection ──────────────────────────────────────────────────────

export interface DbView {
  viewname: string;
  definition: string;
}

/** Get all views in the schema */
export async function getExistingViews(client: pg.PoolClient, pgSchema: string): Promise<ViewSchema[]> {
  const res = await client.query<DbView>(
    `SELECT viewname, definition FROM pg_views WHERE schemaname = $1 ORDER BY viewname`,
    [pgSchema],
  );
  return res.rows.map((r) => ({
    name: r.viewname,
    query: r.definition.replace(/;$/, "").trim(),
  }));
}

// ─── Materialized View Introspection ─────────────────────────────────────────

export interface DbMatView {
  matviewname: string;
  definition: string;
}

/** Get all materialized views in the schema */
export async function getExistingMaterializedViews(client: pg.PoolClient, pgSchema: string): Promise<MaterializedViewSchema[]> {
  const res = await client.query<DbMatView>(
    `SELECT matviewname, definition FROM pg_matviews WHERE schemaname = $1 ORDER BY matviewname`,
    [pgSchema],
  );

  const mvs: MaterializedViewSchema[] = [];
  for (const row of res.rows) {
    const mv: MaterializedViewSchema = {
      name: row.matviewname,
      query: row.definition.replace(/;$/, "").trim(),
    };

    // Get indexes on this materialized view
    const idxRes = await client.query<DbIndex>(
      `SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = $1 AND tablename = $2`,
      [pgSchema, row.matviewname],
    );

    if (idxRes.rows.length > 0) {
      mv.indexes = idxRes.rows.map((idx) => parseIndexDef(idx.indexdef, row.matviewname));
    }

    mvs.push(mv);
  }

  return mvs;
}

// ─── Index Parsing ───────────────────────────────────────────────────────────

export interface ParsedIndex {
  name: string;
  columns: string[];
  unique: boolean;
  method: string;
  where?: string;
  include?: string[];
  opclass?: string;
}

/** Parse a pg_indexes indexdef string into a structured object */
export function parseIndexDef(indexdef: string, _tableName: string): IndexDef {
  const unique = /\bUNIQUE\b/i.test(indexdef);

  // Extract method (USING btree/gin/gist/hash/brin)
  const methodMatch = indexdef.match(/USING\s+(\w+)/i);
  const method = methodMatch ? methodMatch[1].toLowerCase() : "btree";

  // Extract columns from parentheses after USING method or ON table
  const colMatch = indexdef.match(/\(([^)]+)\)/);
  const colStr = colMatch ? colMatch[1] : "";
  const columns = colStr.split(",").map((c) => c.trim());

  // Extract WHERE clause
  const whereMatch = indexdef.match(/WHERE\s+(.+)$/i);
  const where = whereMatch ? whereMatch[1] : undefined;

  // Extract INCLUDE
  const includeMatch = indexdef.match(/INCLUDE\s+\(([^)]+)\)/i);
  const include = includeMatch ? includeMatch[1].split(",").map((c) => c.trim()) : undefined;

  // Extract index name
  const nameMatch = indexdef.match(/INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?"?(\w+)"?/i);
  const name = nameMatch ? nameMatch[1] : undefined;

  const idx: IndexDef = { columns };
  if (name) idx.name = name;
  if (unique) idx.unique = true;
  if (method !== "btree") idx.method = method;
  if (where) idx.where = where;
  if (include) idx.include = include;

  return idx;
}

/** Parse indexdef to a ParsedIndex with full details */
export function parseIndexDefFull(indexdef: string): ParsedIndex {
  const unique = /\bUNIQUE\b/i.test(indexdef);
  const methodMatch = indexdef.match(/USING\s+(\w+)/i);
  const method = methodMatch ? methodMatch[1].toLowerCase() : "btree";

  const colMatch = indexdef.match(/\(([^)]+)\)/);
  const colStr = colMatch ? colMatch[1] : "";
  const columns = colStr.split(",").map((c) => c.trim());

  const whereMatch = indexdef.match(/WHERE\s+(.+)$/i);
  const where = whereMatch ? whereMatch[1] : undefined;

  const includeMatch = indexdef.match(/INCLUDE\s+\(([^)]+)\)/i);
  const include = includeMatch ? includeMatch[1].split(",").map((c) => c.trim()) : undefined;

  const nameMatch = indexdef.match(/INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?"?(\w+)"?/i);
  const name = nameMatch ? nameMatch[1] : "unknown";

  return { name, columns, unique, method, where, include };
}

// ─── Comment Introspection ───────────────────────────────────────────────────

/** Get the comment on a table */
export async function getTableComment(client: pg.PoolClient, tableName: string, pgSchema: string): Promise<string | null> {
  const res = await client.query(
    `SELECT obj_description(c.oid) AS comment
     FROM pg_class c
     JOIN pg_namespace n ON c.relnamespace = n.oid
     WHERE n.nspname = $1 AND c.relname = $2`,
    [pgSchema, tableName],
  );
  return res.rows.length > 0 ? res.rows[0].comment : null;
}

/** Get comments on all columns of a table */
export async function getColumnComments(
  client: pg.PoolClient,
  tableName: string,
  pgSchema: string,
): Promise<Map<string, string>> {
  const res = await client.query(
    `SELECT a.attname, col_description(c.oid, a.attnum) AS comment
     FROM pg_class c
     JOIN pg_namespace n ON c.relnamespace = n.oid
     JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
     WHERE n.nspname = $1 AND c.relname = $2 AND col_description(c.oid, a.attnum) IS NOT NULL`,
    [pgSchema, tableName],
  );
  const map = new Map<string, string>();
  for (const row of res.rows) {
    map.set(row.attname, row.comment);
  }
  return map;
}

/** Get the comment on an enum type */
export async function getEnumComment(client: pg.PoolClient, enumName: string, pgSchema: string): Promise<string | null> {
  const res = await client.query(
    `SELECT obj_description(t.oid, 'pg_type') AS comment
     FROM pg_type t
     JOIN pg_namespace n ON t.typnamespace = n.oid
     WHERE n.nspname = $1 AND t.typname = $2`,
    [pgSchema, enumName],
  );
  return res.rows.length > 0 ? res.rows[0].comment : null;
}

/** Get the comment on a view */
export async function getViewComment(client: pg.PoolClient, viewName: string, pgSchema: string): Promise<string | null> {
  const res = await client.query(
    `SELECT obj_description(c.oid, 'pg_class') AS comment
     FROM pg_class c
     JOIN pg_namespace n ON c.relnamespace = n.oid
     WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'v'`,
    [pgSchema, viewName],
  );
  return res.rows.length > 0 ? res.rows[0].comment : null;
}

/** Get the comment on a materialized view */
export async function getMaterializedViewComment(client: pg.PoolClient, mvName: string, pgSchema: string): Promise<string | null> {
  const res = await client.query(
    `SELECT obj_description(c.oid, 'pg_class') AS comment
     FROM pg_class c
     JOIN pg_namespace n ON c.relnamespace = n.oid
     WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind = 'm'`,
    [pgSchema, mvName],
  );
  return res.rows.length > 0 ? res.rows[0].comment : null;
}

/** Get the comment on a function */
export async function getFunctionComment(client: pg.PoolClient, funcName: string, pgSchema: string): Promise<string | null> {
  const res = await client.query(
    `SELECT obj_description(p.oid, 'pg_proc') AS comment
     FROM pg_proc p
     JOIN pg_namespace n ON p.pronamespace = n.oid
     WHERE n.nspname = $1 AND p.proname = $2
     LIMIT 1`,
    [pgSchema, funcName],
  );
  return res.rows.length > 0 ? res.rows[0].comment : null;
}

/** Get comments on indexes of a table */
export async function getIndexComments(
  client: pg.PoolClient,
  tableName: string,
  pgSchema: string,
): Promise<Map<string, string>> {
  const res = await client.query(
    `SELECT ci.relname AS indexname, obj_description(ci.oid, 'pg_class') AS comment
     FROM pg_index i
     JOIN pg_class ci ON i.indexrelid = ci.oid
     JOIN pg_class ct ON i.indrelid = ct.oid
     JOIN pg_namespace n ON ct.relnamespace = n.oid
     WHERE n.nspname = $1 AND ct.relname = $2 AND obj_description(ci.oid, 'pg_class') IS NOT NULL`,
    [pgSchema, tableName],
  );
  const map = new Map<string, string>();
  for (const row of res.rows) {
    map.set(row.indexname, row.comment);
  }
  return map;
}

/** Get comments on triggers of a table */
export async function getTriggerComments(
  client: pg.PoolClient,
  tableName: string,
  pgSchema: string,
): Promise<Map<string, string>> {
  const res = await client.query(
    `SELECT t.tgname AS triggername, obj_description(t.oid, 'pg_trigger') AS comment
     FROM pg_trigger t
     JOIN pg_class c ON t.tgrelid = c.oid
     JOIN pg_namespace n ON c.relnamespace = n.oid
     WHERE n.nspname = $1 AND c.relname = $2 AND NOT t.tgisinternal AND obj_description(t.oid, 'pg_trigger') IS NOT NULL`,
    [pgSchema, tableName],
  );
  const map = new Map<string, string>();
  for (const row of res.rows) {
    map.set(row.triggername, row.comment);
  }
  return map;
}

/** Get comments on constraints of a table */
export async function getConstraintComments(
  client: pg.PoolClient,
  tableName: string,
  pgSchema: string,
): Promise<Map<string, string>> {
  const res = await client.query(
    `SELECT c.conname AS constraintname, obj_description(c.oid, 'pg_constraint') AS comment
     FROM pg_constraint c
     JOIN pg_class t ON c.conrelid = t.oid
     JOIN pg_namespace n ON t.relnamespace = n.oid
     WHERE n.nspname = $1 AND t.relname = $2 AND obj_description(c.oid, 'pg_constraint') IS NOT NULL`,
    [pgSchema, tableName],
  );
  const map = new Map<string, string>();
  for (const row of res.rows) {
    map.set(row.constraintname, row.comment);
  }
  return map;
}

/** Get comments on policies of a table */
export async function getPolicyComments(
  client: pg.PoolClient,
  tableName: string,
  pgSchema: string,
): Promise<Map<string, string>> {
  const res = await client.query(
    `SELECT p.polname AS policyname, obj_description(p.oid, 'pg_policy') AS comment
     FROM pg_policy p
     JOIN pg_class c ON p.polrelid = c.oid
     JOIN pg_namespace n ON c.relnamespace = n.oid
     WHERE n.nspname = $1 AND c.relname = $2 AND obj_description(p.oid, 'pg_policy') IS NOT NULL`,
    [pgSchema, tableName],
  );
  const map = new Map<string, string>();
  for (const row of res.rows) {
    map.set(row.policyname, row.comment);
  }
  return map;
}

// ─── Generated Column Introspection ──────────────────────────────────────────

/** Get generated column expressions for a table */
export async function getGeneratedColumns(
  client: pg.PoolClient,
  tableName: string,
  pgSchema: string,
): Promise<Map<string, string>> {
  const res = await client.query(
    `SELECT column_name, generation_expression
     FROM information_schema.columns
     WHERE table_schema = $1 AND table_name = $2 AND is_generated = 'ALWAYS'`,
    [pgSchema, tableName],
  );
  const map = new Map<string, string>();
  for (const row of res.rows) {
    if (row.generation_expression) {
      map.set(row.column_name, row.generation_expression);
    }
  }
  return map;
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
