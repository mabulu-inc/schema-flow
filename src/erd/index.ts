// src/erd/index.ts
// Generate Mermaid ER diagrams from YAML schema definitions

import { parseTableFile } from "../schema/parser.js";
import { loadMixins, expandMixins } from "../schema/mixins.js";
import { discoverSchemaFiles } from "../core/files.js";
import type { TableSchema, ColumnDef } from "../schema/types.js";
import path from "node:path";

export interface ErdOptions {
  /** Include column types in the diagram (default: true) */
  showTypes?: boolean;
}

/** Sanitize a type string for Mermaid (no commas, parens are ok but commas break parsing) */
function sanitizeType(type: string): string {
  return type.replace(/,/g, "_");
}

/** Determine the column marker: PK, FK, UK, or empty */
function columnMarker(col: ColumnDef): string {
  if (col.primary_key) return "PK";
  if (col.references) return "FK";
  if (col.unique) return "UK";
  return "";
}

/** Generate Mermaid ER diagram from parsed schemas */
export function generateMermaidErd(schemas: TableSchema[], options?: ErdOptions): string {
  const showTypes = options?.showTypes !== false;
  const lines: string[] = ["erDiagram"];

  // Collect FK relationships
  const relationships: string[] = [];

  for (const schema of schemas) {
    const tableName = schema.table.toUpperCase();
    lines.push(`    ${tableName} {`);

    for (const col of schema.columns) {
      const marker = columnMarker(col);
      if (showTypes) {
        const sanitized = sanitizeType(col.type);
        lines.push(`        ${sanitized} ${col.name}${marker ? ` ${marker}` : ""}`);
      } else {
        lines.push(`        ${col.name}${marker ? ` ${marker}` : ""}`);
      }

      // Collect FK relationship
      if (col.references) {
        const refTable = col.references.table.toUpperCase();
        // Determine cardinality: unique FK = one-to-one, otherwise one-to-many
        const cardinality = col.unique ? "||--||" : "||--o{";
        relationships.push(`    ${refTable} ${cardinality} ${tableName} : "${col.name}"`);
      }
    }

    lines.push("    }");
  }

  // Add relationships after all entities
  for (const rel of relationships) {
    lines.push(rel);
  }

  return lines.join("\n") + "\n";
}

/** Generate Mermaid ER diagram directly from schema files on disk */
export async function generateErdFromFiles(
  schemaDir: string,
  mixinsDir: string,
  options?: ErdOptions,
): Promise<string> {
  const schemaFiles = await discoverSchemaFiles(schemaDir);

  // Filter out non-table files
  const NON_TABLE_PREFIXES = ["fn_", "enum_", "view_", "mv_", "role_"];
  const tableFiles = schemaFiles.filter((f) => {
    const base = path.basename(f);
    return (
      !NON_TABLE_PREFIXES.some((p) => base.startsWith(p)) && base !== "extensions.yaml" && base !== "extensions.yml"
    );
  });

  const schemas = tableFiles.map((f) => parseTableFile(f));

  // Expand mixins
  const mixinMap = await loadMixins(mixinsDir);
  const expanded = expandMixins(schemas, mixinMap);

  return generateMermaidErd(expanded, options);
}
