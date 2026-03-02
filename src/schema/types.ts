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
}
