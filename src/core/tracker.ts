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
  phase: "pre" | "schema" | "post" | "repeatable";
}

const TRACKER_DDL = `
CREATE TABLE IF NOT EXISTS %TABLE% (
  file_path  TEXT PRIMARY KEY,
  file_hash  TEXT NOT NULL,
  phase      TEXT NOT NULL CHECK (phase IN ('pre', 'schema', 'post', 'repeatable')),
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
    // Migrate existing tables: add 'repeatable' to the phase CHECK constraint
    await client.query(`
      DO $$ BEGIN
        ALTER TABLE ${this.table} DROP CONSTRAINT IF EXISTS ${this.table}_phase_check;
        ALTER TABLE ${this.table} ADD CONSTRAINT ${this.table}_phase_check
          CHECK (phase IN ('pre', 'schema', 'post', 'repeatable'));
      EXCEPTION WHEN others THEN NULL;
      END $$;
    `);
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
    _phase: "pre" | "schema" | "post" | "repeatable",
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
  async recordFile(
    client: pg.PoolClient,
    filePath: string,
    phase: "pre" | "schema" | "post" | "repeatable",
  ): Promise<void> {
    const hash = this.hashFile(filePath);
    await client.query(
      `INSERT INTO ${this.table} (file_path, file_hash, phase)
       VALUES ($1, $2, $3)
       ON CONFLICT (file_path) DO UPDATE
       SET file_hash = EXCLUDED.file_hash, applied_at = now()`,
      [filePath, hash, phase],
    );
  }

  // ─── Run Tracking ─────────────────────────────────────────────────────

  private get runsTable(): string {
    return `${this.table}_runs`;
  }

  /** Ensure the runs tracking table exists */
  async ensureRunsTable(client: pg.PoolClient): Promise<void> {
    await client.query(`
      CREATE TABLE IF NOT EXISTS ${this.runsTable} (
        run_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
        completed_at TIMESTAMPTZ,
        status       TEXT NOT NULL CHECK (status IN ('running','completed','failed','rolled_back')),
        operations   JSONB NOT NULL DEFAULT '[]',
        snapshot     JSONB NOT NULL DEFAULT '{}',
        ops_executed INTEGER NOT NULL DEFAULT 0
      );
    `);
    logger.debug(`Ensured runs table: ${this.runsTable}`);
  }

  /** Start a new migration run, returning the run_id */
  async startRun(client: pg.PoolClient, operations: unknown[], snapshot: unknown): Promise<string> {
    const res = await client.query(
      `INSERT INTO ${this.runsTable} (status, operations, snapshot)
       VALUES ('running', $1::jsonb, $2::jsonb)
       RETURNING run_id::text`,
      [JSON.stringify(operations), JSON.stringify(snapshot)],
    );
    return res.rows[0].run_id;
  }

  /** Mark a run as completed */
  async completeRun(client: pg.PoolClient, runId: string, opsExecuted: number): Promise<void> {
    await client.query(
      `UPDATE ${this.runsTable}
       SET status = 'completed', completed_at = now(), ops_executed = $2
       WHERE run_id = $1::uuid`,
      [runId, opsExecuted],
    );
  }

  /** Mark a run as failed */
  async failRun(client: pg.PoolClient, runId: string, opsExecuted: number): Promise<void> {
    await client.query(
      `UPDATE ${this.runsTable}
       SET status = 'failed', completed_at = now(), ops_executed = $2
       WHERE run_id = $1::uuid`,
      [runId, opsExecuted],
    );
  }

  /** Mark a run as rolled back */
  async rollbackRun(client: pg.PoolClient, runId: string): Promise<void> {
    await client.query(
      `UPDATE ${this.runsTable}
       SET status = 'rolled_back', completed_at = now()
       WHERE run_id = $1::uuid`,
      [runId],
    );
  }

  /** Get the last completed run */
  async getLastRun(client: pg.PoolClient): Promise<RunRecord | null> {
    const res = await client.query<RunRecord>(
      `SELECT run_id::text, started_at::text, completed_at::text, status, operations, snapshot, ops_executed
       FROM ${this.runsTable}
       WHERE status = 'completed'
       ORDER BY started_at DESC LIMIT 1`,
    );
    return res.rows[0] || null;
  }

  /** List recent runs */
  async listRuns(client: pg.PoolClient, limit = 10): Promise<RunRecord[]> {
    const res = await client.query<RunRecord>(
      `SELECT run_id::text, started_at::text, completed_at::text, status, operations, snapshot, ops_executed
       FROM ${this.runsTable}
       ORDER BY started_at DESC LIMIT $1`,
      [limit],
    );
    return res.rows;
  }
}

export interface RunRecord {
  run_id: string;
  started_at: string;
  completed_at: string | null;
  status: "running" | "completed" | "failed" | "rolled_back";
  operations: unknown[];
  snapshot: unknown;
  ops_executed: number;
}
