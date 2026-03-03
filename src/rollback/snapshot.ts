// src/rollback/snapshot.ts
// Capture database state snapshot before migration (for rollback)

import pg from "pg";
import { getTableColumns } from "../introspect/index.js";

export interface ColumnSnapshot {
  type: string;
  nullable: boolean;
  default?: string;
}

export interface TableSnapshot {
  columns: Record<string, ColumnSnapshot>;
}

export interface MigrationSnapshot {
  tables: Record<string, TableSnapshot>;
  capturedAt: string;
}

/** Capture a snapshot of the affected tables before migration */
export async function captureSnapshot(
  client: pg.PoolClient,
  affectedTables: string[],
  pgSchema: string,
): Promise<MigrationSnapshot> {
  const tables: Record<string, TableSnapshot> = {};

  for (const tableName of affectedTables) {
    try {
      const cols = await getTableColumns(client, tableName, pgSchema);
      const columns: Record<string, ColumnSnapshot> = {};
      for (const col of cols) {
        columns[col.column_name] = {
          type: col.udt_name,
          nullable: col.is_nullable === "YES",
          default: col.column_default || undefined,
        };
      }
      tables[tableName] = { columns };
    } catch {
      // Table may not exist yet (create_table)
    }
  }

  return {
    tables,
    capturedAt: new Date().toISOString(),
  };
}
