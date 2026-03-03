// src/core/files.ts
// Shared file discovery and utility functions

import path from "node:path";
import { glob } from "glob";

/** Discover and sort SQL scripts in a directory */
export async function discoverScripts(dir: string): Promise<string[]> {
  const patterns = ["*.sql"];
  const files = await glob(patterns.map((p) => path.join(dir, p)));
  return files.sort(); // Alphabetical sort → timestamps in filenames ensure order
}

/** Discover YAML schema files */
export async function discoverSchemaFiles(dir: string): Promise<string[]> {
  const patterns = ["*.yaml", "*.yml"];
  const files = await glob(patterns.map((p) => path.join(dir, p)));
  return files.sort();
}

/** Generate a UTC timestamp string for filenames: YYYYMMDDHHMMSS */
export function utcTimestamp(): string {
  const now = new Date();
  return now
    .toISOString()
    .replace(/[-:T]/g, "")
    .replace(/\.\d+Z/, "")
    .slice(0, 14);
}
