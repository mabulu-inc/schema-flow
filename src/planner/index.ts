// src/planner/index.ts
// Diff engine: compare desired YAML state with current DB state, produce SQL operations
// Key design: CREATE TABLEs first (without FKs), then ALTER TABLEs for FKs last
// Safety: destructive operations (drops, narrowing changes) are blocked by default

import type {
  TableSchema,
  ColumnDef,
  IndexDef,
  CheckDef,
  TriggerDef,
  PolicyDef,
  EnumSchema,
  ExtensionsSchema,
  ViewSchema,
  MaterializedViewSchema,
  UniqueConstraintDef,
  RoleSchema,
  GrantDef,
  FunctionSchema,
} from "../schema/types.js";
import {
  introspectTable,
  getExistingTables,
  getTableConstraints,
  getExistingEnums,
  getExistingExtensions,
  getExistingViews,
  getExistingMaterializedViews,
  getTableIndexes,
  parseIndexDefFull,
  getTableComment,
  getColumnComments,
  getEnumComment,
  getViewComment,
  getMaterializedViewComment,
  getIndexComments,
  getTriggerComments,
  getConstraintComments,
  getPolicyComments,
  getExistingRoles,
  getTableGrants as introspectTableGrants,
  getColumnGrants as introspectColumnGrants,
  getOwnedSequences,
  getSequenceGrants as introspectSequenceGrants,
  getFunctionExecuteGrants as introspectFunctionGrants,
  type ParsedIndex,
} from "../introspect/index.js";
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
  | "drop_dual_write_trigger"
  | "create_enum"
  | "add_enum_value"
  | "create_extension"
  | "drop_extension"
  | "create_view"
  | "drop_view"
  | "create_materialized_view"
  | "drop_materialized_view"
  | "refresh_materialized_view"
  | "set_comment"
  | "create_role"
  | "alter_role"
  | "grant_membership"
  | "grant_table"
  | "grant_column"
  | "revoke_table"
  | "revoke_column"
  | "grant_sequence"
  | "revoke_sequence"
  | "grant_function"
  | "revoke_function"
  | "seed_table";

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
  /** Desired enum schemas */
  enums?: EnumSchema[];
  /** Desired extensions */
  extensions?: ExtensionsSchema;
  /** Desired views */
  views?: ViewSchema[];
  /** Desired materialized views */
  materializedViews?: MaterializedViewSchema[];
  /** Desired roles */
  roles?: RoleSchema[];
  /** Desired functions (for grant planning) */
  functions?: FunctionSchema[];
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

  // Plan roles first (must exist before grants reference them)
  if (options.roles) {
    const roleOps = await planRoles(client, options.roles);
    allOps.push(...roleOps);
  }

  // Plan extensions (execute first)
  if (options.extensions) {
    const extOps = await planExtensions(client, options.extensions, pgSchema);
    allOps.push(...extOps);
  }

  // Plan enums (before tables)
  if (options.enums) {
    const enumOps = await planEnums(client, options.enums, pgSchema);
    allOps.push(...enumOps);
  }

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

  // Plan seeds (after tables are created/altered)
  for (const desired of desiredSchemas) {
    if (desired.seeds && desired.seeds.length > 0) {
      const seedOps = planSeeds(desired, pgSchema);
      allOps.push(...seedOps);
    }
  }

  // Plan grants (after tables are created/altered, roles exist)
  for (const desired of desiredSchemas) {
    if (desired.grants && desired.grants.length > 0) {
      const grantOps = await planGrantDiff(client, desired.table, desired.grants, pgSchema, desired.columns);
      allOps.push(...grantOps);
    }
  }

  // Plan views (after tables)
  if (options.views) {
    const viewOps = await planViews(client, options.views, pgSchema);
    allOps.push(...viewOps);
  }

  // Plan materialized views (after tables)
  if (options.materializedViews) {
    const mvOps = await planMaterializedViews(client, options.materializedViews, pgSchema);
    allOps.push(...mvOps);
  }

  // Plan grants for views (after views are created)
  if (options.views) {
    for (const view of options.views) {
      if (view.grants && view.grants.length > 0) {
        const grantOps = await planGrantDiff(client, view.name, view.grants, pgSchema);
        allOps.push(...grantOps);
      }
    }
  }

  // Plan grants for materialized views (after MVs are created)
  if (options.materializedViews) {
    for (const mv of options.materializedViews) {
      if (mv.grants && mv.grants.length > 0) {
        const grantOps = await planGrantDiff(client, mv.name, mv.grants, pgSchema);
        allOps.push(...grantOps);
      }
    }
  }

  // Plan grants for functions (after functions are created)
  if (options.functions) {
    for (const fn of options.functions) {
      if (fn.grants && fn.grants.length > 0) {
        const grantOps = await planFunctionGrantDiff(client, fn, pgSchema);
        allOps.push(...grantOps);
      }
    }
    // Revoke grants for functions that have no grants in YAML
    for (const fn of options.functions) {
      if (!fn.grants || fn.grants.length === 0) {
        const revokeOps = await planFunctionGrantDiff(client, fn, pgSchema);
        allOps.push(...revokeOps);
      }
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
  // Within structure ops, reorder so all create_table ops come before
  // enable_rls and create_policy ops. This prevents failures when a policy
  // on table B references table A via subquery and B is planned first.
  const rawStructureOps = allowed.filter((op) => op.phase === "structure");
  const isRoleRelated = (o: Operation) =>
    o.type === "create_role" || o.type === "alter_role" || o.type === "grant_membership";
  const isPolicyRelated = (o: Operation) =>
    o.type === "enable_rls" ||
    o.type === "create_policy" ||
    (o.type === "set_comment" && o.sql.includes("COMMENT ON POLICY"));
  const isGrantRelated = (o: Operation) =>
    o.type === "grant_table" || o.type === "grant_column" || o.type === "revoke_table" || o.type === "revoke_column";
  const structureOps = [
    // Roles first (before tables, so grants can reference them)
    ...rawStructureOps.filter((o) => isRoleRelated(o)),
    // Normal structure ops (create_table, add_column, etc.)
    ...rawStructureOps.filter((o) => !isRoleRelated(o) && !isPolicyRelated(o) && !isGrantRelated(o)),
    // Enable RLS
    ...rawStructureOps.filter((o) => o.type === "enable_rls"),
    // Create policies
    ...rawStructureOps.filter(
      (o) => o.type === "create_policy" || (o.type === "set_comment" && o.sql.includes("COMMENT ON POLICY")),
    ),
    // Grants last (tables and roles must exist)
    ...rawStructureOps.filter((o) => isGrantRelated(o)),
  ];
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

    if (col.generated) {
      def += ` GENERATED ALWAYS AS (${col.generated}) STORED`;
    } else {
      if (col.primary_key) {
        if (schema.primary_key_name) {
          def += ` CONSTRAINT "${schema.primary_key_name}" PRIMARY KEY`;
        } else {
          def += " PRIMARY KEY";
        }
      }
      if (col.unique) {
        if (col.unique_name) {
          def += ` CONSTRAINT "${col.unique_name}" UNIQUE`;
        } else {
          def += " UNIQUE";
        }
      }
      if (col.nullable === false || col.primary_key) {
        if (!col.primary_key) def += " NOT NULL";
      } else if (col.nullable === true) {
        // Nullable is the default, no need to add anything
      } else if (!col.primary_key) {
        def += " NOT NULL"; // Convention: NOT NULL by default
      }
      if (col.default !== undefined) def += ` DEFAULT ${col.default}`;
    }

    colDefs.push(def);

    // Collect foreign keys for deferred application (NOT VALID + VALIDATE two-step)
    if (col.references) {
      const fkName = col.references.name || `fk_${schema.table}_${col.name}_${col.references.table}`;
      const onDelete = col.references.on_delete || "NO ACTION";
      const onUpdate = col.references.on_update || "NO ACTION";
      const deferrable = col.references.deferrable ? " DEFERRABLE" : "";
      const initiallyDeferred = col.references.initially_deferred ? " INITIALLY DEFERRED" : "";
      fkOps.push({
        type: "add_foreign_key_not_valid",
        table: schema.table,
        sql: `ALTER TABLE "${pgSchema}"."${schema.table}" ADD CONSTRAINT "${fkName}" FOREIGN KEY ("${col.name}") REFERENCES "${pgSchema}"."${col.references.table}" ("${col.references.column}") ON DELETE ${onDelete} ON UPDATE ${onUpdate}${deferrable}${initiallyDeferred} NOT VALID;`,
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
    if (schema.primary_key_name) {
      colDefs.push(`  CONSTRAINT "${schema.primary_key_name}" PRIMARY KEY (${pkCols})`);
    } else {
      colDefs.push(`  PRIMARY KEY (${pkCols})`);
    }
  }

  // Multi-column unique constraints
  // Constraints with expression columns (COALESCE, casts, etc.) cannot use
  // inline UNIQUE — they need a separate CREATE UNIQUE INDEX after the table.
  const deferredUniqueConstraints: typeof schema.unique_constraints = [];
  if (schema.unique_constraints) {
    for (const uc of schema.unique_constraints) {
      if (uc.columns.some(isExpression)) {
        deferredUniqueConstraints.push(uc);
      } else {
        const ucCols = uc.columns.map((c) => `"${c}"`).join(", ");
        if (uc.name) {
          colDefs.push(`  CONSTRAINT "${uc.name}" UNIQUE (${ucCols})`);
        } else {
          colDefs.push(`  UNIQUE (${ucCols})`);
        }
      }
    }
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

  // Deferred unique constraints with expression columns (emitted as CREATE UNIQUE INDEX)
  for (const uc of deferredUniqueConstraints) {
    const idxName = uc.name || `uq_${schema.table}_${uc.columns.join("_")}`;
    const ucCols = formatIndexColumns(uc.columns);
    ops.push({
      type: "add_unique_index",
      table: schema.table,
      sql: `CREATE UNIQUE INDEX IF NOT EXISTS "${idxName}" ON "${pgSchema}"."${schema.table}" (${ucCols});`,
      description: `Create unique index ${idxName} on ${schema.table}`,
      phase: "structure",
      destructive: false,
    });
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

  // Comments for new table objects (table, columns, triggers, checks, policies)
  const commentOps = planTableComments(
    schema.table,
    schema,
    pgSchema,
    null,
    new Map(),
    new Map(),
    new Map(),
    new Map(),
    new Map(),
  );
  // Index comments must run in validate phase (after CONCURRENTLY index creation)
  for (const op of commentOps) {
    if (op.sql.includes("COMMENT ON INDEX")) {
      op.phase = "validate";
    }
  }
  ops.push(...commentOps);

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
        const fkName = col.references.name || `fk_${desired.table}_${col.name}_${col.references.table}`;
        const onDelete = col.references.on_delete || "NO ACTION";
        const onUpdate = col.references.on_update || "NO ACTION";
        const deferrable = col.references.deferrable ? " DEFERRABLE" : "";
        const initiallyDeferred = col.references.initially_deferred ? " INITIALLY DEFERRED" : "";
        ops.push({
          type: "add_foreign_key_not_valid",
          table: desired.table,
          sql: `ALTER TABLE "${pgSchema}"."${desired.table}" ADD CONSTRAINT "${fkName}" FOREIGN KEY ("${col.name}") REFERENCES "${pgSchema}"."${col.references.table}" ("${col.references.column}") ON DELETE ${onDelete} ON UPDATE ${onUpdate}${deferrable}${initiallyDeferred} NOT VALID;`,
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
        const constraintName = col.unique_name || `uq_${desired.table}_${col.name}`;
        const idxName = `idx_${desired.table}_${col.name}_unique`;
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
      const fkName = col.references.name || `fk_${desired.table}_${col.name}_${col.references.table}`;
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
        const deferrable = col.references.deferrable ? " DEFERRABLE" : "";
        const initiallyDeferred = col.references.initially_deferred ? " INITIALLY DEFERRED" : "";
        ops.push({
          type: "add_foreign_key_not_valid",
          table: desired.table,
          sql: `ALTER TABLE "${pgSchema}"."${desired.table}" ADD CONSTRAINT "${fkName}" FOREIGN KEY ("${col.name}") REFERENCES "${pgSchema}"."${col.references.table}" ("${col.references.column}") ON DELETE ${onDelete} ON UPDATE ${onUpdate}${deferrable}${initiallyDeferred} NOT VALID;`,
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

  // Index diff
  const indexOps = await planIndexDiff(client, desired.table, desired.indexes || [], pgSchema);
  ops.push(...indexOps);

  // Multi-column unique constraint diff
  if (desired.unique_constraints) {
    const ucOps = await planUniqueConstraintDiff(client, desired.table, desired.unique_constraints, pgSchema);
    ops.push(...ucOps);
  }

  // Comment diff
  const currentTableComment = await getTableComment(client, desired.table, pgSchema);
  const currentColumnComments = await getColumnComments(client, desired.table, pgSchema);
  const currentIndexComments = await getIndexComments(client, desired.table, pgSchema);
  const currentTriggerComments = await getTriggerComments(client, desired.table, pgSchema);
  const currentConstraintComments = await getConstraintComments(client, desired.table, pgSchema);
  const currentPolicyComments = await getPolicyComments(client, desired.table, pgSchema);
  const commentOps = planTableComments(
    desired.table,
    desired,
    pgSchema,
    currentTableComment,
    currentColumnComments,
    currentIndexComments,
    currentTriggerComments,
    currentConstraintComments,
    currentPolicyComments,
  );
  // Index comments must run in validate phase (after CONCURRENTLY index creation)
  for (const op of commentOps) {
    if (op.sql.includes("COMMENT ON INDEX")) {
      op.phase = "validate";
    }
  }
  ops.push(...commentOps);

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
    const constraintName = desired.unique_name || `uq_${tableName}_${desired.name}`;
    const idxName = `idx_${tableName}_${desired.name}_unique`;
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
  const method = idx.method && idx.method !== "btree" ? ` USING ${idx.method}` : "";
  const cols = formatIndexColumns(idx.columns);
  const opclass = idx.opclass ? ` ${idx.opclass}` : "";
  const include = idx.include ? ` INCLUDE (${idx.include.map((c) => `"${c}"`).join(", ")})` : "";
  const where = idx.where ? ` WHERE ${idx.where}` : "";

  return {
    type: "add_index",
    table: tableName,
    sql: `CREATE ${unique}INDEX CONCURRENTLY IF NOT EXISTS "${idxName}" ON "${pgSchema}"."${tableName}"${method} (${cols}${opclass})${include}${where};`,
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

function planPolicyDiff(tableName: string, desired: PolicyDef[], current: PolicyDef[], pgSchema: string): Operation[] {
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
  if (col.generated) {
    sql += ` GENERATED ALWAYS AS (${col.generated}) STORED`;
  } else {
    if (col.nullable === false) sql += " NOT NULL";
    if (col.unique) sql += " UNIQUE";
    if (col.default !== undefined) sql += ` DEFAULT ${col.default}`;
  }
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

// ─── Enum Planning ───────────────────────────────────────────────────────────

async function planEnums(client: pg.PoolClient, desiredEnums: EnumSchema[], pgSchema: string): Promise<Operation[]> {
  const ops: Operation[] = [];
  const existingEnums = await getExistingEnums(client, pgSchema);
  const existingMap = new Map(existingEnums.map((e) => [e.name, e]));

  for (const desired of desiredEnums) {
    const existing = existingMap.get(desired.name);
    if (!existing) {
      // Create new enum
      const values = desired.values.map((v) => `'${v}'`).join(", ");
      ops.push({
        type: "create_enum",
        sql: `CREATE TYPE "${pgSchema}"."${desired.name}" AS ENUM (${values});`,
        description: `Create enum type ${desired.name}`,
        phase: "structure",
        destructive: false,
      });
    } else {
      // Diff values — PG only supports ADD VALUE (no removes without drop+recreate)
      for (const val of desired.values) {
        if (!existing.values.includes(val)) {
          ops.push({
            type: "add_enum_value",
            sql: `ALTER TYPE "${pgSchema}"."${desired.name}" ADD VALUE IF NOT EXISTS '${val}';`,
            description: `Add value '${val}' to enum ${desired.name}`,
            phase: "structure",
            destructive: false,
          });
        }
      }
    }

    // Enum comment
    if (desired.comment !== undefined) {
      const currentComment = await getEnumComment(client, desired.name, pgSchema);
      if (desired.comment !== currentComment) {
        ops.push({
          type: "set_comment",
          sql: `COMMENT ON TYPE "${pgSchema}"."${desired.name}" IS '${desired.comment.replace(/'/g, "''")}';`,
          description: `Set comment on enum ${desired.name}`,
          phase: "structure",
          destructive: false,
        });
      }
    }
  }

  return ops;
}

// ─── Extension Planning ──────────────────────────────────────────────────────

async function planExtensions(
  client: pg.PoolClient,
  desired: ExtensionsSchema,
  _pgSchema: string,
): Promise<Operation[]> {
  const ops: Operation[] = [];
  const existing = new Set(await getExistingExtensions(client));

  for (const ext of desired.extensions) {
    if (!existing.has(ext)) {
      ops.push({
        type: "create_extension",
        sql: `CREATE EXTENSION IF NOT EXISTS "${ext}";`,
        description: `Create extension ${ext}`,
        phase: "structure",
        destructive: false,
      });
    }
  }

  return ops;
}

// ─── View Planning ───────────────────────────────────────────────────────────

async function planViews(client: pg.PoolClient, desiredViews: ViewSchema[], pgSchema: string): Promise<Operation[]> {
  const ops: Operation[] = [];
  const existingViews = await getExistingViews(client, pgSchema);
  const existingMap = new Map(existingViews.map((v) => [v.name, v]));

  for (const desired of desiredViews) {
    const existing = existingMap.get(desired.name);
    if (!existing) {
      ops.push({
        type: "create_view",
        sql: `CREATE OR REPLACE VIEW "${pgSchema}"."${desired.name}" AS ${desired.query};`,
        description: `Create view ${desired.name}`,
        phase: "structure",
        destructive: false,
      });
    } else {
      // Compare query text (normalized)
      const desiredNorm = desired.query.replace(/\s+/g, " ").trim();
      const existingNorm = existing.query.replace(/\s+/g, " ").trim();
      if (desiredNorm !== existingNorm) {
        ops.push({
          type: "create_view",
          sql: `CREATE OR REPLACE VIEW "${pgSchema}"."${desired.name}" AS ${desired.query};`,
          description: `Update view ${desired.name}`,
          phase: "structure",
          destructive: false,
        });
      }
    }

    // View comment
    if (desired.comment !== undefined) {
      const currentComment = await getViewComment(client, desired.name, pgSchema);
      if (desired.comment !== currentComment) {
        ops.push({
          type: "set_comment",
          sql: `COMMENT ON VIEW "${pgSchema}"."${desired.name}" IS '${desired.comment.replace(/'/g, "''")}';`,
          description: `Set comment on view ${desired.name}`,
          phase: "structure",
          destructive: false,
        });
      }
    }
  }

  return ops;
}

// ─── Materialized View Planning ──────────────────────────────────────────────

async function planMaterializedViews(
  client: pg.PoolClient,
  desiredMvs: MaterializedViewSchema[],
  pgSchema: string,
): Promise<Operation[]> {
  const ops: Operation[] = [];
  const existingMvs = await getExistingMaterializedViews(client, pgSchema);
  const existingMap = new Map(existingMvs.map((v) => [v.name, v]));

  for (const desired of desiredMvs) {
    const existing = existingMap.get(desired.name);
    if (!existing) {
      // Create
      ops.push({
        type: "create_materialized_view",
        sql: `CREATE MATERIALIZED VIEW "${pgSchema}"."${desired.name}" AS ${desired.query};`,
        description: `Create materialized view ${desired.name}`,
        phase: "structure",
        destructive: false,
      });
      // Add indexes
      if (desired.indexes) {
        for (const idx of desired.indexes) {
          const idxName = idx.name || `idx_${desired.name}_${idx.columns.join("_")}`;
          const unique = idx.unique ? "UNIQUE " : "";
          const method = idx.method && idx.method !== "btree" ? ` USING ${idx.method}` : "";
          const cols = formatIndexColumns(idx.columns);
          const where = idx.where ? ` WHERE ${idx.where}` : "";
          ops.push({
            type: "add_index",
            sql: `CREATE ${unique}INDEX "${idxName}" ON "${pgSchema}"."${desired.name}"${method} (${cols})${where};`,
            description: `Create index ${idxName} on materialized view ${desired.name}`,
            phase: "structure",
            destructive: false,
          });
        }
      }
    } else {
      // Compare query
      const desiredNorm = desired.query.replace(/\s+/g, " ").trim();
      const existingNorm = existing.query.replace(/\s+/g, " ").trim();
      if (desiredNorm !== existingNorm) {
        // Must drop+recreate for mat views
        ops.push({
          type: "drop_materialized_view",
          sql: `DROP MATERIALIZED VIEW IF EXISTS "${pgSchema}"."${desired.name}";`,
          description: `Drop materialized view ${desired.name} for recreation`,
          phase: "structure",
          destructive: false,
        });
        ops.push({
          type: "create_materialized_view",
          sql: `CREATE MATERIALIZED VIEW "${pgSchema}"."${desired.name}" AS ${desired.query};`,
          description: `Recreate materialized view ${desired.name}`,
          phase: "structure",
          destructive: false,
        });
        if (desired.indexes) {
          for (const idx of desired.indexes) {
            const idxName = idx.name || `idx_${desired.name}_${idx.columns.join("_")}`;
            const unique = idx.unique ? "UNIQUE " : "";
            const method = idx.method && idx.method !== "btree" ? ` USING ${idx.method}` : "";
            const cols = formatIndexColumns(idx.columns);
            const where = idx.where ? ` WHERE ${idx.where}` : "";
            ops.push({
              type: "add_index",
              sql: `CREATE ${unique}INDEX "${idxName}" ON "${pgSchema}"."${desired.name}"${method} (${cols})${where};`,
              description: `Create index ${idxName} on materialized view ${desired.name}`,
              phase: "structure",
              destructive: false,
            });
          }
        }
      }
    }

    // Materialized view comment
    if (desired.comment !== undefined) {
      const currentComment = await getMaterializedViewComment(client, desired.name, pgSchema);
      if (desired.comment !== currentComment) {
        ops.push({
          type: "set_comment",
          sql: `COMMENT ON MATERIALIZED VIEW "${pgSchema}"."${desired.name}" IS '${desired.comment.replace(/'/g, "''")}';`,
          description: `Set comment on materialized view ${desired.name}`,
          phase: "structure",
          destructive: false,
        });
      }
    }
  }

  return ops;
}

// ─── Index Diffing ───────────────────────────────────────────────────────────

async function planIndexDiff(
  client: pg.PoolClient,
  tableName: string,
  desiredIndexes: IndexDef[],
  pgSchema: string,
): Promise<Operation[]> {
  const ops: Operation[] = [];
  const dbIndexes = await getTableIndexes(client, tableName, pgSchema);

  // Filter out constraint-backing indexes using semantic metadata from pg_constraint
  const standaloneIndexes = dbIndexes.filter((i) => !i.constraint_type);
  const existingParsed: ParsedIndex[] = standaloneIndexes.map((i) => parseIndexDefFull(i.indexdef));

  // Build maps by name
  const existingByName = new Map(existingParsed.map((i) => [i.name, i]));
  const desiredByName = new Map<string, IndexDef>();
  for (const idx of desiredIndexes) {
    const name = idx.name || `idx_${tableName}_${idx.columns.join("_")}`;
    desiredByName.set(name, idx);
  }

  // New indexes
  for (const [name, idx] of desiredByName) {
    if (!existingByName.has(name)) {
      ops.push(planCreateIndex(tableName, { ...idx, name }, pgSchema));
    } else {
      // Check if changed
      const existing = existingByName.get(name)!;
      const desiredMethod = idx.method || "btree";
      const changed =
        existing.method !== desiredMethod ||
        normalizeIndexColumns(existing.columns).join(",") !== normalizeIndexColumns(idx.columns).join(",") ||
        Boolean(existing.unique) !== Boolean(idx.unique) ||
        normalizeWhere(existing.where) !== normalizeWhere(idx.where);

      if (changed) {
        // Drop and recreate
        ops.push({
          type: "drop_index",
          table: tableName,
          sql: `DROP INDEX CONCURRENTLY IF EXISTS "${pgSchema}"."${name}";`,
          description: `Drop index ${name} on ${tableName} for recreation`,
          phase: "structure",
          destructive: false,
        });
        ops.push(planCreateIndex(tableName, { ...idx, name }, pgSchema));
      }
    }
  }

  // Removed indexes (destructive)
  for (const [name] of existingByName) {
    if (!desiredByName.has(name)) {
      ops.push({
        type: "drop_index",
        table: tableName,
        sql: `DROP INDEX CONCURRENTLY IF EXISTS "${pgSchema}"."${name}";`,
        description: `Drop index ${name} on ${tableName}`,
        phase: "structure",
        destructive: true,
      });
    }
  }

  return ops;
}

async function planUniqueConstraintDiff(
  client: pg.PoolClient,
  tableName: string,
  desiredConstraints: UniqueConstraintDef[],
  pgSchema: string,
): Promise<Operation[]> {
  const ops: Operation[] = [];

  // Get existing unique constraints from pg_constraint
  const dbConstraints = await getTableConstraints(client, tableName, pgSchema);
  const existingUniqueMap = new Map<string, string[]>();
  for (const c of dbConstraints) {
    if (c.constraint_type === "UNIQUE") {
      if (!existingUniqueMap.has(c.constraint_name)) existingUniqueMap.set(c.constraint_name, []);
      existingUniqueMap.get(c.constraint_name)!.push(c.column_name);
    }
  }

  for (const uc of desiredConstraints) {
    const constraintName = uc.name || `uq_${tableName}_${uc.columns.join("_")}`;
    if (existingUniqueMap.has(constraintName)) continue;

    // Use concurrent index pattern for safe addition
    const ucCols = formatIndexColumns(uc.columns);
    const idxName = `idx_${tableName}_${uc.columns.join("_")}_unique`;
    ops.push({
      type: "add_unique_index",
      table: tableName,
      sql: `CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "${idxName}" ON "${pgSchema}"."${tableName}" (${ucCols});`,
      description: `Create unique index ${idxName} on ${tableName}(${uc.columns.join(", ")})`,
      phase: "structure",
      destructive: false,
    });
    ops.push({
      type: "alter_column",
      table: tableName,
      sql: `ALTER TABLE "${pgSchema}"."${tableName}" ADD CONSTRAINT "${constraintName}" UNIQUE USING INDEX "${idxName}";`,
      description: `Add unique constraint ${constraintName} on ${tableName}`,
      phase: "validate",
      destructive: false,
    });
  }

  return ops;
}

function normalizeIndexColumns(cols: string[]): string[] {
  return cols.map((c) =>
    c
      .replace(/^"(.*)"$/, "$1")
      .trim()
      .toLowerCase(),
  );
}

function normalizeWhere(w?: string): string {
  return (w || "").replace(/\s+/g, " ").trim().toLowerCase();
}

// ─── Comment Planning ────────────────────────────────────────────────────────

export function planTableComments(
  tableName: string,
  desired: TableSchema,
  pgSchema: string,
  currentTableComment: string | null,
  currentColumnComments: Map<string, string>,
  currentIndexComments?: Map<string, string>,
  currentTriggerComments?: Map<string, string>,
  currentConstraintComments?: Map<string, string>,
  currentPolicyComments?: Map<string, string>,
): Operation[] {
  const ops: Operation[] = [];
  const qualifiedTable = `"${pgSchema}"."${tableName}"`;

  // Table comment
  if (desired.comment !== undefined && desired.comment !== currentTableComment) {
    ops.push({
      type: "set_comment",
      table: tableName,
      sql: `COMMENT ON TABLE ${qualifiedTable} IS '${desired.comment.replace(/'/g, "''")}';`,
      description: `Set comment on table ${tableName}`,
      phase: "structure",
      destructive: false,
    });
  }

  // Column comments
  for (const col of desired.columns) {
    if (col.comment !== undefined) {
      const currentComment = currentColumnComments.get(col.name) || null;
      if (col.comment !== currentComment) {
        ops.push({
          type: "set_comment",
          table: tableName,
          sql: `COMMENT ON COLUMN ${qualifiedTable}."${col.name}" IS '${col.comment.replace(/'/g, "''")}';`,
          description: `Set comment on ${tableName}.${col.name}`,
          phase: "structure",
          destructive: false,
        });
      }
    }
  }

  // Index comments
  if (desired.indexes && currentIndexComments) {
    for (const idx of desired.indexes) {
      if (idx.comment !== undefined) {
        const idxName = idx.name || `idx_${tableName}_${idx.columns.join("_")}`;
        const currentComment = currentIndexComments.get(idxName) || null;
        if (idx.comment !== currentComment) {
          ops.push({
            type: "set_comment",
            table: tableName,
            sql: `COMMENT ON INDEX "${pgSchema}"."${idxName}" IS '${idx.comment.replace(/'/g, "''")}';`,
            description: `Set comment on index ${idxName}`,
            phase: "structure",
            destructive: false,
          });
        }
      }
    }
  }

  // Trigger comments
  if (desired.triggers && currentTriggerComments) {
    for (const trigger of desired.triggers) {
      if (trigger.comment !== undefined) {
        const currentComment = currentTriggerComments.get(trigger.name) || null;
        if (trigger.comment !== currentComment) {
          ops.push({
            type: "set_comment",
            table: tableName,
            sql: `COMMENT ON TRIGGER "${trigger.name}" ON ${qualifiedTable} IS '${trigger.comment.replace(/'/g, "''")}';`,
            description: `Set comment on trigger ${trigger.name}`,
            phase: "structure",
            destructive: false,
          });
        }
      }
    }
  }

  // Check constraint comments
  if (desired.checks && currentConstraintComments) {
    for (const check of desired.checks) {
      if (check.comment !== undefined) {
        const checkName = check.name || `chk_${tableName}_${Date.now()}`;
        const currentComment = currentConstraintComments.get(checkName) || null;
        if (check.comment !== currentComment) {
          ops.push({
            type: "set_comment",
            table: tableName,
            sql: `COMMENT ON CONSTRAINT "${checkName}" ON ${qualifiedTable} IS '${check.comment.replace(/'/g, "''")}';`,
            description: `Set comment on constraint ${checkName}`,
            phase: "structure",
            destructive: false,
          });
        }
      }
    }
  }

  // Policy comments
  if (desired.policies && currentPolicyComments) {
    for (const policy of desired.policies) {
      if (policy.comment !== undefined) {
        const currentComment = currentPolicyComments.get(policy.name) || null;
        if (policy.comment !== currentComment) {
          ops.push({
            type: "set_comment",
            table: tableName,
            sql: `COMMENT ON POLICY "${policy.name}" ON ${qualifiedTable} IS '${policy.comment.replace(/'/g, "''")}';`,
            description: `Set comment on policy ${policy.name}`,
            phase: "structure",
            destructive: false,
          });
        }
      }
    }
  }

  return ops;
}

// ─── Helper: Format index columns ────────────────────────────────────────────

/** Check if a column reference is an expression (contains function calls, casts, etc.) */
function isExpression(col: string): boolean {
  return col.includes("(") || col.includes("::") || col.includes(" ");
}

/** Format columns for CREATE INDEX, quoting identifiers but not expressions */
function formatIndexColumns(columns: string[]): string {
  return columns.map((c) => (isExpression(c) ? c : `"${c}"`)).join(", ");
}

// ─── Role Planning ───────────────────────────────────────────────────────────

async function planRoles(client: pg.PoolClient, desiredRoles: RoleSchema[]): Promise<Operation[]> {
  const ops: Operation[] = [];
  const existingRoles = await getExistingRoles(client);
  const existingMap = new Map(existingRoles.map((r) => [r.role, r]));

  // Sort: roles without memberships first, then roles with memberships
  const sorted = [...desiredRoles].sort((a, b) => {
    const aHasDeps = (a.in?.length || 0) > 0 ? 1 : 0;
    const bHasDeps = (b.in?.length || 0) > 0 ? 1 : 0;
    return aHasDeps - bHasDeps;
  });

  for (const desired of sorted) {
    const existing = existingMap.get(desired.role);
    if (!existing) {
      // Create role
      const attrs = buildRoleAttributes(desired);
      ops.push({
        type: "create_role",
        sql: `CREATE ROLE "${desired.role}"${attrs};`,
        description: `Create role ${desired.role}`,
        phase: "structure",
        destructive: false,
      });
    } else {
      // Diff attributes
      const alterClauses = diffRoleAttributes(desired, existing);
      if (alterClauses.length > 0) {
        ops.push({
          type: "alter_role",
          sql: `ALTER ROLE "${desired.role}" ${alterClauses.join(" ")};`,
          description: `Alter role ${desired.role}: ${alterClauses.join(", ")}`,
          phase: "structure",
          destructive: false,
        });
      }
    }

    // Memberships
    if (desired.in) {
      const existingMemberships = new Set(existing?.in || []);
      for (const group of desired.in) {
        if (!existingMemberships.has(group)) {
          ops.push({
            type: "grant_membership",
            sql: `GRANT "${group}" TO "${desired.role}";`,
            description: `Grant role ${group} to ${desired.role}`,
            phase: "structure",
            destructive: false,
          });
        }
      }
    }
  }

  return ops;
}

function buildRoleAttributes(role: RoleSchema): string {
  const parts: string[] = [];
  if (role.login === true) parts.push("LOGIN");
  else if (role.login === false) parts.push("NOLOGIN");
  if (role.superuser === true) parts.push("SUPERUSER");
  else if (role.superuser === false) parts.push("NOSUPERUSER");
  if (role.createdb === true) parts.push("CREATEDB");
  else if (role.createdb === false) parts.push("NOCREATEDB");
  if (role.createrole === true) parts.push("CREATEROLE");
  else if (role.createrole === false) parts.push("NOCREATEROLE");
  if (role.inherit === false) parts.push("NOINHERIT");
  else if (role.inherit === true) parts.push("INHERIT");
  if (role.connection_limit !== undefined && role.connection_limit >= 0) {
    parts.push(`CONNECTION LIMIT ${role.connection_limit}`);
  }
  return parts.length > 0 ? ` ${parts.join(" ")}` : "";
}

function diffRoleAttributes(desired: RoleSchema, existing: RoleSchema): string[] {
  const clauses: string[] = [];
  const desiredLogin = desired.login ?? false;
  const desiredSuperuser = desired.superuser ?? false;
  const desiredCreatedb = desired.createdb ?? false;
  const desiredCreaterole = desired.createrole ?? false;
  const desiredInherit = desired.inherit ?? true;

  if (desiredLogin !== existing.login) clauses.push(desiredLogin ? "LOGIN" : "NOLOGIN");
  if (desiredSuperuser !== existing.superuser) clauses.push(desiredSuperuser ? "SUPERUSER" : "NOSUPERUSER");
  if (desiredCreatedb !== existing.createdb) clauses.push(desiredCreatedb ? "CREATEDB" : "NOCREATEDB");
  if (desiredCreaterole !== existing.createrole) clauses.push(desiredCreaterole ? "CREATEROLE" : "NOCREATEROLE");
  if (desiredInherit !== existing.inherit) clauses.push(desiredInherit ? "INHERIT" : "NOINHERIT");

  if (desired.connection_limit !== undefined && desired.connection_limit !== existing.connection_limit) {
    clauses.push(`CONNECTION LIMIT ${desired.connection_limit}`);
  }

  return clauses;
}

// ─── Grant Planning ──────────────────────────────────────────────────────────

async function planGrantDiff(
  client: pg.PoolClient,
  tableName: string,
  desiredGrants: GrantDef[],
  pgSchema: string,
  columns?: ColumnDef[],
): Promise<Operation[]> {
  const ops: Operation[] = [];

  // Introspect existing grants
  const existingTableGrants = await introspectTableGrants(client, tableName, pgSchema);
  const existingColGrants = await introspectColumnGrants(client, tableName, pgSchema);

  // Build maps: role → Set<privilege> for table-level
  const existingTableMap = new Map<string, Set<string>>();
  for (const g of existingTableGrants) {
    if (!existingTableMap.has(g.grantee)) existingTableMap.set(g.grantee, new Set());
    existingTableMap.get(g.grantee)!.add(g.privilege_type);
  }

  // Build maps: role → Map<column, Set<privilege>> for column-level
  const existingColMap = new Map<string, Map<string, Set<string>>>();
  for (const g of existingColGrants) {
    if (!existingColMap.has(g.grantee)) existingColMap.set(g.grantee, new Map());
    const roleMap = existingColMap.get(g.grantee)!;
    if (!roleMap.has(g.column_name)) roleMap.set(g.column_name, new Set());
    roleMap.get(g.column_name)!.add(g.privilege_type);
  }

  // Track which role+privilege combos are desired (for revoke detection)
  const desiredTablePrivs = new Map<string, Set<string>>();
  const desiredColPrivs = new Map<string, Map<string, Set<string>>>();

  for (const grant of desiredGrants) {
    const roles = Array.isArray(grant.to) ? grant.to : [grant.to];

    for (const role of roles) {
      if (grant.columns) {
        // Column-level grant
        if (!desiredColPrivs.has(role)) desiredColPrivs.set(role, new Map());
        const roleColMap = desiredColPrivs.get(role)!;

        for (const col of grant.columns) {
          if (!roleColMap.has(col)) roleColMap.set(col, new Set());
          for (const priv of grant.privileges) {
            roleColMap.get(col)!.add(priv);
          }
        }

        // Check what we need to grant
        const existingRoleColMap = existingColMap.get(role);
        for (const col of grant.columns) {
          for (const priv of grant.privileges) {
            const existingPrivs = existingRoleColMap?.get(col);
            if (!existingPrivs?.has(priv)) {
              // Need to grant — batch by privilege
              ops.push({
                type: "grant_column",
                table: tableName,
                sql: `GRANT ${priv} ("${col}") ON "${pgSchema}"."${tableName}" TO "${role}";`,
                description: `Grant ${priv} on ${tableName}(${col}) to ${role}`,
                phase: "structure",
                destructive: false,
              });
            }
          }
        }
      } else {
        // Table-level grant
        if (!desiredTablePrivs.has(role)) desiredTablePrivs.set(role, new Set());
        for (const priv of grant.privileges) {
          desiredTablePrivs.get(role)!.add(priv);
        }

        const existingPrivs = existingTableMap.get(role);
        for (const priv of grant.privileges) {
          if (!existingPrivs?.has(priv)) {
            ops.push({
              type: "grant_table",
              table: tableName,
              sql: `GRANT ${priv} ON "${pgSchema}"."${tableName}" TO "${role}";`,
              description: `Grant ${priv} on ${tableName} to ${role}`,
              phase: "structure",
              destructive: false,
            });
          }
        }
      }
    }
  }

  // Revoke table-level privileges that exist but are not desired
  for (const [role, existingPrivs] of existingTableMap) {
    const desired = desiredTablePrivs.get(role);
    if (!desired) {
      // All table privs for this role should be revoked
      for (const priv of existingPrivs) {
        ops.push({
          type: "revoke_table",
          table: tableName,
          sql: `REVOKE ${priv} ON "${pgSchema}"."${tableName}" FROM "${role}";`,
          description: `Revoke ${priv} on ${tableName} from ${role}`,
          phase: "structure",
          destructive: true,
        });
      }
    } else {
      for (const priv of existingPrivs) {
        if (!desired.has(priv)) {
          ops.push({
            type: "revoke_table",
            table: tableName,
            sql: `REVOKE ${priv} ON "${pgSchema}"."${tableName}" FROM "${role}";`,
            description: `Revoke ${priv} on ${tableName} from ${role}`,
            phase: "structure",
            destructive: true,
          });
        }
      }
    }
  }

  // ─── Auto sequence grants ─────────────────────────────────────────────────
  // When INSERT or ALL is granted on a table, auto-grant USAGE + SELECT on
  // owned sequences (serial/bigserial columns). Without this, INSERT fails
  // because the role can't advance the sequence.
  // Introspect owned sequences from DB; fall back to deriving from column defs
  // (needed for new tables where the table doesn't exist in DB yet)
  let ownedSeqs = await getOwnedSequences(client, tableName, pgSchema);
  if (ownedSeqs.length === 0 && columns) {
    const serialTypes = new Set(["serial", "bigserial", "smallserial"]);
    ownedSeqs = columns.filter((c) => serialTypes.has(c.type.toLowerCase())).map((c) => `${tableName}_${c.name}_seq`);
  }
  if (ownedSeqs.length > 0) {
    // Collect roles that need sequence access (have INSERT or ALL)
    const rolesNeedingSeqs = new Set<string>();
    for (const [role, privs] of desiredTablePrivs) {
      if (privs.has("INSERT") || privs.has("ALL")) {
        rolesNeedingSeqs.add(role);
      }
    }

    // Collect roles that previously had sequence access but no longer need it
    const rolesLosingSeqs = new Set<string>();
    for (const [role] of existingTableMap) {
      const hadInsert = existingTableMap.get(role)?.has("INSERT") || existingTableMap.get(role)?.has("ALL");
      if (hadInsert && !rolesNeedingSeqs.has(role)) {
        rolesLosingSeqs.add(role);
      }
    }

    for (const seqName of ownedSeqs) {
      const existingSeqGrants = await introspectSequenceGrants(client, seqName, pgSchema);
      const existingSeqMap = new Map<string, Set<string>>();
      for (const g of existingSeqGrants) {
        if (!existingSeqMap.has(g.grantee)) existingSeqMap.set(g.grantee, new Set());
        existingSeqMap.get(g.grantee)!.add(g.privilege_type);
      }

      // Grant USAGE + SELECT to roles that need it
      for (const role of rolesNeedingSeqs) {
        const existing = existingSeqMap.get(role);
        for (const seqPriv of ["USAGE", "SELECT"]) {
          if (!existing?.has(seqPriv)) {
            ops.push({
              type: "grant_sequence",
              table: tableName,
              sql: `GRANT ${seqPriv} ON SEQUENCE "${pgSchema}"."${seqName}" TO "${role}";`,
              description: `Grant ${seqPriv} on sequence ${seqName} to ${role} (auto: table INSERT)`,
              phase: "structure",
              destructive: false,
            });
          }
        }
      }

      // Revoke from roles that lost INSERT
      for (const role of rolesLosingSeqs) {
        const existing = existingSeqMap.get(role);
        if (existing) {
          for (const seqPriv of existing) {
            ops.push({
              type: "revoke_sequence",
              table: tableName,
              sql: `REVOKE ${seqPriv} ON SEQUENCE "${pgSchema}"."${seqName}" FROM "${role}";`,
              description: `Revoke ${seqPriv} on sequence ${seqName} from ${role} (auto: table INSERT removed)`,
              phase: "structure",
              destructive: true,
            });
          }
        }
      }
    }
  }

  return ops;
}

// ─── Function Grant Planning ─────────────────────────────────────────────────

async function planFunctionGrantDiff(
  client: pg.PoolClient,
  fn: FunctionSchema,
  pgSchema: string,
): Promise<Operation[]> {
  const ops: Operation[] = [];
  const argsClause = fn.args ? `(${fn.args})` : "()";
  const qualifiedName = `"${pgSchema}"."${fn.name}"${argsClause}`;

  // Introspect existing function grants
  const existingGrants = await introspectFunctionGrants(client, fn.name, pgSchema);
  const existingMap = new Map<string, Set<string>>();
  for (const g of existingGrants) {
    if (!existingMap.has(g.grantee)) existingMap.set(g.grantee, new Set());
    existingMap.get(g.grantee)!.add(g.privilege_type);
  }

  // Track desired role+privilege combos
  const desiredPrivs = new Map<string, Set<string>>();

  for (const grant of fn.grants || []) {
    const roles = Array.isArray(grant.to) ? grant.to : [grant.to];
    for (const role of roles) {
      if (!desiredPrivs.has(role)) desiredPrivs.set(role, new Set());
      for (const priv of grant.privileges) {
        desiredPrivs.get(role)!.add(priv);

        const existingRolePrivs = existingMap.get(role);
        if (!existingRolePrivs?.has(priv)) {
          ops.push({
            type: "grant_function",
            sql: `GRANT ${priv} ON FUNCTION ${qualifiedName} TO "${role}";`,
            description: `Grant ${priv} on function ${fn.name} to ${role}`,
            phase: "structure",
            destructive: false,
          });
        }
      }
    }
  }

  // Revoke privileges that exist but are not desired
  for (const [role, existingPrivSet] of existingMap) {
    const desired = desiredPrivs.get(role);
    if (!desired) {
      for (const priv of existingPrivSet) {
        ops.push({
          type: "revoke_function",
          sql: `REVOKE ${priv} ON FUNCTION ${qualifiedName} FROM "${role}";`,
          description: `Revoke ${priv} on function ${fn.name} from ${role}`,
          phase: "structure",
          destructive: true,
        });
      }
    } else {
      for (const priv of existingPrivSet) {
        if (!desired.has(priv)) {
          ops.push({
            type: "revoke_function",
            sql: `REVOKE ${priv} ON FUNCTION ${qualifiedName} FROM "${role}";`,
            description: `Revoke ${priv} on function ${fn.name} from ${role}`,
            phase: "structure",
            destructive: true,
          });
        }
      }
    }
  }

  return ops;
}

// ---------------------------------------------------------------------------
// Seed data — UPDATE where changed + INSERT where missing
// ---------------------------------------------------------------------------

/** Escape a value for use in a SQL VALUES clause */
function seedLiteral(value: unknown): string {
  if (value === null || value === undefined) return "NULL";
  if (typeof value === "boolean") return value ? "TRUE" : "FALSE";
  if (typeof value === "number") return String(value);
  return `'${String(value).replace(/'/g, "''")}'`;
}

/**
 * Resolve the primary key column names for a table schema.
 * Checks both column-level `primary_key: true` and table-level `primary_key: [...]`.
 */
function getPkColumns(schema: TableSchema): string[] {
  if (schema.primary_key && schema.primary_key.length > 0) {
    return schema.primary_key;
  }
  return schema.columns.filter((c) => c.primary_key).map((c) => c.name);
}

/**
 * Resolve the key columns used to match seed rows against existing data.
 * Prefers PK columns when present in seed data; falls back to unique columns.
 */
function getSeedKeyColumns(schema: TableSchema, seedCols: string[]): string[] {
  // Try PK first
  const pkCols = getPkColumns(schema);
  if (pkCols.length > 0 && pkCols.every((c) => seedCols.includes(c))) {
    return pkCols;
  }
  // Fall back to a unique column present in seed data
  for (const col of schema.columns) {
    if (col.unique && seedCols.includes(col.name)) {
      return [col.name];
    }
  }
  return [];
}

/**
 * Generate seed operations: UPDATE existing rows where data differs,
 * INSERT missing rows with a NOT EXISTS guard to avoid consuming serials.
 */
function planSeeds(schema: TableSchema, pgSchema: string): Operation[] {
  const seeds = schema.seeds!;

  // Collect all columns referenced in any seed row
  const allSeedCols = [...new Set(seeds.flatMap((row) => Object.keys(row)))];
  const keyCols = getSeedKeyColumns(schema, allSeedCols);
  if (keyCols.length === 0) {
    logger.warn(`Table "${schema.table}" has seeds but no primary key or unique column in seed data — skipping seeds`);
    return [];
  }
  const nonKeyCols = allSeedCols.filter((c) => !keyCols.includes(c));
  const qt = `"${pgSchema}"."${schema.table}"`;

  // Build VALUES clause shared by both statements
  const valuesRows = seeds.map((row) => {
    const vals = allSeedCols.map((col) => seedLiteral(row[col]));
    return `  (${vals.join(", ")})`;
  });
  const valuesList = valuesRows.join(",\n");
  const colAliases = allSeedCols.map((c) => `"${c}"`).join(", ");
  const valuesClause = `(VALUES\n${valuesList}\n) AS v(${colAliases})`;

  const stmts: string[] = [];

  // UPDATE: only rows where at least one non-key column differs
  if (nonKeyCols.length > 0) {
    const setCols = nonKeyCols.map((c) => `"${c}" = v."${c}"`).join(", ");
    const whereKey = keyCols.map((c) => `t."${c}" = v."${c}"`).join(" AND ");
    const distinctCheck =
      `row(${nonKeyCols.map((c) => `t."${c}"`).join(", ")})` +
      ` IS DISTINCT FROM ` +
      `row(${nonKeyCols.map((c) => `v."${c}"`).join(", ")})`;
    stmts.push(`UPDATE ${qt} AS t SET ${setCols}\nFROM ${valuesClause}\nWHERE ${whereKey}\n  AND ${distinctCheck}`);
  }

  // INSERT: only rows whose key doesn't exist yet
  const insertCols = allSeedCols.map((c) => `"${c}"`).join(", ");
  const selectCols = allSeedCols.map((c) => `v."${c}"`).join(", ");
  const existsKey = keyCols.map((c) => `t."${c}" = v."${c}"`).join(" AND ");
  stmts.push(
    `INSERT INTO ${qt} (${insertCols})\nSELECT ${selectCols}\nFROM ${valuesClause}\nWHERE NOT EXISTS (SELECT 1 FROM ${qt} AS t WHERE ${existsKey})`,
  );

  // Reset serial sequences to max(col) so subsequent inserts don't conflict
  for (const col of schema.columns) {
    if (isSerialType(col.type) && allSeedCols.includes(col.name)) {
      stmts.push(
        `SELECT setval(pg_get_serial_sequence('"${pgSchema}"."${schema.table}"', '${col.name}'), greatest((SELECT max("${col.name}") FROM ${qt}), 1))`,
      );
    }
  }

  const sql = stmts.join(";\n") + ";";
  return [
    {
      type: "seed_table" as OperationType,
      table: schema.table,
      sql,
      description: `Seed ${seeds.length} row(s) into ${schema.table}`,
      phase: "structure",
      destructive: false,
    },
  ];
}
