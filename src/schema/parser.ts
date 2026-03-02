// src/schema/parser.ts
// Parse YAML schema files into typed definitions

import { readFileSync } from "node:fs";
import path from "node:path";
import { parse as parseYaml } from "yaml";
import type { TableSchema, FunctionSchema, ColumnDef } from "./types.js";
import { logger } from "../core/logger.js";

export function parseTableFile(filePath: string): TableSchema {
  const content = readFileSync(filePath, "utf-8");
  const raw = parseYaml(content);

  if (!raw || typeof raw !== "object") {
    throw new Error(`Invalid schema file: ${filePath} — expected a YAML object`);
  }

  // Derive table name from filename if not specified
  const tableName = raw.table || path.basename(filePath, path.extname(filePath));

  if (!raw.columns || !Array.isArray(raw.columns)) {
    throw new Error(`Schema file ${filePath} must define "columns" as an array`);
  }

  const columns: ColumnDef[] = raw.columns.map((col: Record<string, unknown>) => {
    if (!col.name || !col.type) {
      throw new Error(`Each column in ${filePath} must have "name" and "type"`);
    }
    return {
      name: col.name as string,
      type: col.type as string,
      nullable: col.nullable !== undefined ? Boolean(col.nullable) : undefined,
      default: col.default !== undefined ? String(col.default) : undefined,
      primary_key: col.primary_key ? Boolean(col.primary_key) : undefined,
      unique: col.unique ? Boolean(col.unique) : undefined,
      references: col.references
        ? {
            table: (col.references as Record<string, string>).table,
            column: (col.references as Record<string, string>).column,
            on_delete: (col.references as Record<string, string>).on_delete as ColumnDef["references"] extends { on_delete?: infer T } ? T : never,
            on_update: (col.references as Record<string, string>).on_update as ColumnDef["references"] extends { on_update?: infer T } ? T : never,
          }
        : undefined,
    };
  });

  const schema: TableSchema = {
    table: tableName,
    columns,
  };

  if (raw.primary_key && Array.isArray(raw.primary_key)) {
    schema.primary_key = raw.primary_key as string[];
  }

  if (raw.indexes && Array.isArray(raw.indexes)) {
    schema.indexes = raw.indexes;
  }

  if (raw.checks && Array.isArray(raw.checks)) {
    schema.checks = raw.checks;
  }

  logger.debug(`Parsed schema for table: ${schema.table}`, { columns: schema.columns.length });
  return schema;
}

export function parseFunctionFile(filePath: string): FunctionSchema {
  const content = readFileSync(filePath, "utf-8");
  const raw = parseYaml(content);

  if (!raw?.name || !raw?.body) {
    throw new Error(`Function schema file ${filePath} must define "name" and "body"`);
  }

  return {
    name: raw.name,
    language: raw.language || "plpgsql",
    returns: raw.returns || "void",
    args: raw.args || "",
    body: raw.body,
    replace: raw.replace !== false,
  };
}
