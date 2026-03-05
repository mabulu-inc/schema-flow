// src/schema/types.ts
// Type definitions for declarative YAML table schemas

export interface ExpandDef {
  /** Source column name */
  from: string;
  /** SQL expression to transform old → new */
  transform: string;
  /** SQL expression to reverse (new → old), for dual-write trigger */
  reverse?: string;
  /** Batch size for backfill (default: 1000) */
  batch_size?: number;
}

export interface ColumnDef {
  name: string;
  type: string;
  nullable?: boolean;
  default?: string;
  primary_key?: boolean;
  unique?: boolean;
  /** Name of the single-column unique constraint */
  unique_name?: string;
  references?: {
    /** Schema of the referenced table (omit for same-schema FKs) */
    schema?: string;
    table: string;
    column: string;
    /** FK constraint name */
    name?: string;
    on_delete?: "CASCADE" | "SET NULL" | "SET DEFAULT" | "RESTRICT" | "NO ACTION";
    on_update?: "CASCADE" | "SET NULL" | "SET DEFAULT" | "RESTRICT" | "NO ACTION";
    /** Whether the FK constraint is validated (true) or NOT VALID (false). Populated by introspection. */
    validated?: boolean;
    /** Whether the FK constraint is deferrable */
    deferrable?: boolean;
    /** Whether the FK constraint is initially deferred */
    initially_deferred?: boolean;
  };
  /** Expand/contract: column rename/transform via dual-write trigger */
  expand?: ExpandDef;
  /** SQL expression for a generated (stored) column */
  generated?: string;
  /** Column description/comment */
  comment?: string;
}

export interface IndexDef {
  name?: string;
  columns: string[];
  unique?: boolean;
  where?: string;
  /** Index method: btree (default), gin, gist, hash, brin */
  method?: string;
  /** INCLUDE columns for covering indexes */
  include?: string[];
  /** Operator class (e.g., jsonb_path_ops) */
  opclass?: string;
  /** Index description/comment */
  comment?: string;
}

export interface CheckDef {
  name?: string;
  expression: string;
  /** Constraint description/comment */
  comment?: string;
}

export interface UniqueConstraintDef {
  name?: string;
  columns: string[];
  /** Constraint description/comment */
  comment?: string;
}

export interface TriggerDef {
  /** Trigger name */
  name: string;
  /** When the trigger fires */
  timing: "BEFORE" | "AFTER" | "INSTEAD OF";
  /** Events that fire the trigger */
  events: ("INSERT" | "UPDATE" | "DELETE" | "TRUNCATE")[];
  /** Function to execute */
  function: string;
  /** Trigger granularity */
  for_each: "ROW" | "STATEMENT";
  /** Optional WHEN condition */
  when?: string;
  /** Trigger description/comment */
  comment?: string;
}

export interface PolicyDef {
  /** Policy name */
  name: string;
  /** Command the policy applies to */
  for: "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "ALL";
  /** Roles the policy applies to (omit or empty for PUBLIC) */
  to?: string[];
  /** USING expression */
  using?: string;
  /** WITH CHECK expression */
  check?: string;
  /** Whether the policy is permissive (default true); false = RESTRICTIVE */
  permissive?: boolean;
  /** Policy description/comment */
  comment?: string;
}

export interface MixinSchema {
  /** Mixin name (derived from filename if not specified) */
  mixin: string;
  /** Columns to add */
  columns?: ColumnDef[];
  /** Indexes to add */
  indexes?: IndexDef[];
  /** Check constraints to add */
  checks?: CheckDef[];
  /** Triggers to add */
  triggers?: TriggerDef[];
  /** Enable row-level security on tables using this mixin */
  rls?: boolean;
  /** Force row-level security (applies even to table owner) */
  force_rls?: boolean;
  /** RLS policies to add */
  policies?: PolicyDef[];
  /** Table grants to add */
  grants?: GrantDef[];
}

export interface PrecheckDef {
  /** Check name */
  name: string;
  /** SQL query that must return a truthy value */
  query: string;
  /** Custom failure message */
  message?: string;
}

export interface TableSchema {
  /** Table name (derived from filename if not specified) */
  table: string;
  /** Column definitions */
  columns: ColumnDef[];
  /** Primary key columns (if composite; single-column PK uses column-level primary_key) */
  primary_key?: string[];
  /** PK constraint name */
  primary_key_name?: string;
  /** Indexes */
  indexes?: IndexDef[];
  /** Check constraints */
  checks?: CheckDef[];
  /** Multi-column unique constraints */
  unique_constraints?: UniqueConstraintDef[];
  /** Triggers */
  triggers?: TriggerDef[];
  /** Mixin names to apply */
  use?: string[];
  /** Enable row-level security */
  rls?: boolean;
  /** Force row-level security (applies even to table owner) */
  force_rls?: boolean;
  /** RLS policies */
  policies?: PolicyDef[];
  /** Table grants */
  grants?: GrantDef[];
  /** Pre-migration checks: SQL assertions that must pass before migration */
  prechecks?: PrecheckDef[];
  /** Seed rows to insert/update on every migration */
  seeds?: Record<string, unknown>[];
  /** Table description/comment */
  comment?: string;
}

export interface ForeignKeyAction {
  table: string;
  column: string;
  referencesTable: string;
  referencesColumn: string;
  constraintName: string;
  onDelete: string;
  onUpdate: string;
}

export interface FunctionSchema {
  name: string;
  language: string;
  returns: string;
  args?: string;
  body: string;
  replace?: boolean;
  /** Security mode: "definer" generates SECURITY DEFINER, "invoker" is the default */
  security?: "definer" | "invoker";
  /** Function grants (EXECUTE) */
  grants?: FunctionGrantDef[];
  /** Function description/comment */
  comment?: string;
}

export interface FunctionGrantDef {
  /** Role(s) to grant to */
  to: string | string[];
  /** Privileges to grant (only EXECUTE is valid for functions) */
  privileges: "EXECUTE"[];
}

export interface EnumSchema {
  /** Enum type name */
  name: string;
  /** Ordered list of enum values */
  values: string[];
  /** Enum type description/comment */
  comment?: string;
}

export interface ExtensionsSchema {
  /** List of PostgreSQL extensions to enable */
  extensions: string[];
}

export interface ViewSchema {
  /** View name */
  name: string;
  /** SQL query defining the view */
  query: string;
  /** View grants */
  grants?: GrantDef[];
  /** View description/comment */
  comment?: string;
}

export interface MaterializedViewSchema {
  /** Materialized view name */
  name: string;
  /** SQL query defining the materialized view */
  query: string;
  /** Indexes on the materialized view */
  indexes?: IndexDef[];
  /** Materialized view grants */
  grants?: GrantDef[];
  /** Materialized view description/comment */
  comment?: string;
}

export interface RoleSchema {
  /** Role name */
  role: string;
  /** Whether the role can log in (default false) */
  login?: boolean;
  /** Superuser privilege (default false) */
  superuser?: boolean;
  /** Can create databases (default false) */
  createdb?: boolean;
  /** Can create roles (default false) */
  createrole?: boolean;
  /** Inherits privileges of granted roles (default true) */
  inherit?: boolean;
  /** Maximum connections (-1 = unlimited, default -1) */
  connection_limit?: number;
  /** Role memberships — GRANT <role> TO this role */
  in?: string[];
  /** Role description/comment */
  comment?: string;
}

export type GrantPrivilege = "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "TRUNCATE" | "REFERENCES" | "TRIGGER" | "ALL";

export interface GrantDef {
  /** Role(s) to grant to */
  to: string | string[];
  /** Privileges to grant */
  privileges: GrantPrivilege[];
  /** If specified, grant applies only to these columns (column-level grant) */
  columns?: string[];
  /** WITH GRANT OPTION (default false) */
  with_grant_option?: boolean;
}
