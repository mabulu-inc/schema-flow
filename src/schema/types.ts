// src/schema/types.ts
// Type definitions for declarative YAML table schemas

export interface ColumnDef {
  name: string;
  type: string;
  nullable?: boolean;
  default?: string;
  primary_key?: boolean;
  unique?: boolean;
  references?: {
    table: string;
    column: string;
    on_delete?: "CASCADE" | "SET NULL" | "SET DEFAULT" | "RESTRICT" | "NO ACTION";
    on_update?: "CASCADE" | "SET NULL" | "SET DEFAULT" | "RESTRICT" | "NO ACTION";
  };
}

export interface IndexDef {
  name?: string;
  columns: string[];
  unique?: boolean;
  where?: string;
}

export interface CheckDef {
  name?: string;
  expression: string;
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
}

export interface TableSchema {
  /** Table name (derived from filename if not specified) */
  table: string;
  /** Column definitions */
  columns: ColumnDef[];
  /** Primary key columns (if composite; single-column PK uses column-level primary_key) */
  primary_key?: string[];
  /** Indexes */
  indexes?: IndexDef[];
  /** Check constraints */
  checks?: CheckDef[];
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
}
