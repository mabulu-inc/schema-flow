// test/rollback-integration.test.ts
// Integration tests for down migrations (require DB)

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { createTestDb } from "./helpers.js";
import { runDown } from "../src/rollback/executor.js";
import { FileTracker } from "../src/core/tracker.js";
import { resolveConfig } from "../src/core/config.js";
import { closePool, withClient } from "../src/core/db.js";

describe("runDown integration", () => {
  let connectionString: string;
  let dbCleanup: () => Promise<void>;

  beforeEach(async () => {
    const db = await createTestDb();
    connectionString = db.connectionString;
    dbCleanup = db.cleanup;
  });

  afterEach(async () => {
    await closePool();
    await dbCleanup();
  });

  it("returns success with no-op when no completed runs exist", async () => {
    const config = resolveConfig({
      connectionString,
      pgSchema: "public",
    });

    const result = await runDown(config);
    expect(result.success).toBe(true);
    expect(result.operationsExecuted).toBe(0);
    expect(result.plan.operations).toHaveLength(0);
  });

  it("shows rollback plan for a completed run (dry run)", async () => {
    // Seed a completed run directly
    const tracker = new FileTracker("_schema_flow_history");
    await withClient(connectionString, async (client) => {
      await tracker.ensureRunsTable(client);
      const runId = await tracker.startRun(
        client,
        [
          {
            type: "create_table",
            table: "test_tbl",
            sql: 'CREATE TABLE "public"."test_tbl" ("id" serial PRIMARY KEY);',
            description: "Create table test_tbl",
            phase: "structure",
            destructive: false,
          },
        ],
        { tables: {}, capturedAt: "" },
      );
      await tracker.completeRun(client, runId, 1);
    });

    const config = resolveConfig({
      connectionString,
      pgSchema: "public",
    });

    const result = await runDown(config, { apply: false });
    expect(result.success).toBe(true);
    expect(result.operationsExecuted).toBe(0); // dry run
    expect(result.plan.operations.length).toBeGreaterThan(0);
    expect(result.plan.operations[0].sql).toContain("DROP TABLE");
  });

  it("blocks destructive rollback without --allow-destructive", async () => {
    const tracker = new FileTracker("_schema_flow_history");
    await withClient(connectionString, async (client) => {
      await tracker.ensureRunsTable(client);
      const runId = await tracker.startRun(
        client,
        [
          {
            type: "create_table",
            table: "test_tbl2",
            sql: 'CREATE TABLE "public"."test_tbl2" ("id" serial PRIMARY KEY);',
            description: "Create table test_tbl2",
            phase: "structure",
            destructive: false,
          },
        ],
        { tables: {}, capturedAt: "" },
      );
      await tracker.completeRun(client, runId, 1);
    });

    const config = resolveConfig({
      connectionString,
      pgSchema: "public",
    });

    const result = await runDown(config, { apply: true, allowDestructive: false });
    expect(result.success).toBe(false);
    expect(result.errors).toContain("Destructive operations blocked");
  });
});
