// src/core/tracker.ts
// File hash tracker: filepath + SHA-256 hash to detect new/changed files

import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import pg from "pg";
import { logger } from "./logger.js";

export interface TrackedFile {
  file_path: string;
  file_hash: string;
  applied_at: string;
  phase: "pre" | "schema" | "post";
}

const TRACKER_DDL = `
CREATE TABLE IF NOT EXISTS %TABLE% (
  file_path  TEXT PRIMARY KEY,
  file_hash  TEXT NOT NULL,
  phase      TEXT NOT NULL CHECK (phase IN ('pre', 'schema', 'post')),
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
`;

export class FileTracker {
  private table: string;

  constructor(historyTable: string) {
    this.table = historyTable;
  }

  /** Ensure the tracking table exists */
  async ensureTable(client: pg.PoolClient): Promise<void> {
    const ddl = TRACKER_DDL.replace("%TABLE%", this.table);
    await client.query(ddl);
    logger.debug(`Ensured tracking table: ${this.table}`);
  }

  /** Compute SHA-256 hash of a file's contents */
  hashFile(filePath: string): string {
    const content = readFileSync(filePath);
    return createHash("sha256").update(content).digest("hex");
  }

  /** Get all tracked files from the database */
  async getTracked(client: pg.PoolClient): Promise<Map<string, TrackedFile>> {
    const res = await client.query<TrackedFile>(
      `SELECT file_path, file_hash, phase, applied_at::text FROM ${this.table}`,
    );
    const map = new Map<string, TrackedFile>();
    for (const row of res.rows) {
      map.set(row.file_path, row);
    }
    return map;
  }

  /** Determine which files are new or changed */
  classifyFiles(
    filePaths: string[],
    tracked: Map<string, TrackedFile>,
    _phase: "pre" | "schema" | "post",
  ): { newFiles: string[]; changedFiles: string[]; unchangedFiles: string[] } {
    const newFiles: string[] = [];
    const changedFiles: string[] = [];
    const unchangedFiles: string[] = [];

    for (const fp of filePaths) {
      const currentHash = this.hashFile(fp);
      const existing = tracked.get(fp);

      if (!existing) {
        newFiles.push(fp);
      } else if (existing.file_hash !== currentHash) {
        changedFiles.push(fp);
      } else {
        unchangedFiles.push(fp);
      }
    }

    return { newFiles, changedFiles, unchangedFiles };
  }

  /** Record a file as applied */
  async recordFile(client: pg.PoolClient, filePath: string, phase: "pre" | "schema" | "post"): Promise<void> {
    const hash = this.hashFile(filePath);
    await client.query(
      `INSERT INTO ${this.table} (file_path, file_hash, phase)
       VALUES ($1, $2, $3)
       ON CONFLICT (file_path) DO UPDATE
       SET file_hash = EXCLUDED.file_hash, applied_at = now()`,
      [filePath, hash, phase],
    );
  }
}
