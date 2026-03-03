// test/expand-integration.test.ts
// Integration tests for expand/contract: tracker, backfill, executor (require DB)

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { useTestClient, createTestDb } from "./helpers.js";
import { ExpandTracker } from "../src/expand/tracker.js";
import { runBackfill } from "../src/expand/backfill.js";
import { runContract, showExpandStatus } from "../src/expand/executor.js";
import { resolveConfig } from "../src/core/config.js";
import { closePool } from "../src/core/db.js";
import pg from "pg";

const { Pool } = pg;

describe("expand integration", () => {
  describe("ExpandTracker", () => {
    const ctx = useTestClient();

    it("creates the expand state table", async () => {
      const tracker = new ExpandTracker();
      await tracker.ensureTable(ctx.client);

      const res = await ctx.client.query(
        `SELECT 1 FROM information_schema.tables
         WHERE table_name = '_schema_flow_expand'`,
      );
      expect(res.rowCount).toBe(1);
    });

    it("registers an expand operation and returns an id", async () => {
      const tracker = new ExpandTracker();
      await tracker.ensureTable(ctx.client);

      const id = await tracker.register(ctx.client, {
        tableName: "users",
        oldColumn: "name",
        newColumn: "full_name",
        transform: "name",
        reverseExpr: "full_name",
        triggerName: "_sf_dw_users_name_full_name",
        functionName: "_sf_dw_users_name_full_name",
        batchSize: 1000,
      });

      expect(id).toBeGreaterThan(0);
    });

    it("retrieves active records (non-contracted)", async () => {
      const tracker = new ExpandTracker();
      await tracker.ensureTable(ctx.client);

      await tracker.register(ctx.client, {
        tableName: "users",
        oldColumn: "name",
        newColumn: "full_name",
        transform: "name",
        triggerName: "_sf_dw_users_name_full_name",
        functionName: "_sf_dw_users_name_full_name",
        batchSize: 1000,
      });

      const active = await tracker.getActive(ctx.client);
      expect(active.length).toBe(1);
      expect(active[0].table_name).toBe("users");
      expect(active[0].status).toBe("expanding");
    });

    it("updates status", async () => {
      const tracker = new ExpandTracker();
      await tracker.ensureTable(ctx.client);

      const id = await tracker.register(ctx.client, {
        tableName: "items",
        oldColumn: "desc",
        newColumn: "description",
        transform: "desc",
        triggerName: "_sf_dw_items_desc_description",
        functionName: "_sf_dw_items_desc_description",
        batchSize: 500,
      });

      await tracker.updateStatus(ctx.client, id, "expanded");
      const record = await tracker.getForColumn(ctx.client, "items", "desc", "description");
      expect(record).not.toBeNull();
      expect(record!.status).toBe("expanded");
    });

    it("getForColumn returns null for contracted records", async () => {
      const tracker = new ExpandTracker();
      await tracker.ensureTable(ctx.client);

      const id = await tracker.register(ctx.client, {
        tableName: "items",
        oldColumn: "desc",
        newColumn: "description",
        transform: "desc",
        triggerName: "_sf_dw_items_desc_description",
        functionName: "_sf_dw_items_desc_description",
        batchSize: 500,
      });

      await tracker.updateStatus(ctx.client, id, "contracted");
      const record = await tracker.getForColumn(ctx.client, "items", "desc", "description");
      expect(record).toBeNull();
    });

    it("getAll returns all records including contracted", async () => {
      const tracker = new ExpandTracker();
      await tracker.ensureTable(ctx.client);

      const id1 = await tracker.register(ctx.client, {
        tableName: "a",
        oldColumn: "x",
        newColumn: "y",
        transform: "x",
        triggerName: "t1",
        functionName: "f1",
        batchSize: 100,
      });
      await tracker.register(ctx.client, {
        tableName: "b",
        oldColumn: "x",
        newColumn: "y",
        transform: "x",
        triggerName: "t2",
        functionName: "f2",
        batchSize: 100,
      });

      await tracker.updateStatus(ctx.client, id1, "contracted");

      const all = await tracker.getAll(ctx.client);
      expect(all.length).toBe(2);
    });
  });

  describe("runBackfill", () => {
    const ctx = useTestClient();

    it("backfills NULL values in batches", async () => {
      await ctx.client.query(`
        CREATE TABLE "public"."bf_test" (
          id serial PRIMARY KEY,
          name text NOT NULL,
          full_name text
        );
      `);

      await ctx.client.query(`
        INSERT INTO "public"."bf_test" (name) VALUES ('alice'), ('bob'), ('charlie'), ('dave'), ('eve');
      `);

      const pool = new Pool({ connectionString: ctx.connectionString, max: 2 });
      try {
        const result = await runBackfill(pool, {
          tableName: "bf_test",
          newColumn: "full_name",
          transform: "name",
          batchSize: 2,
          pgSchema: "public",
        });

        expect(result.totalRows).toBe(5);
        expect(result.batches).toBeGreaterThanOrEqual(3);

        const check = await ctx.client.query(
          `SELECT count(*) as cnt FROM "public"."bf_test" WHERE full_name IS NULL`,
        );
        expect(parseInt(check.rows[0].cnt, 10)).toBe(0);
      } finally {
        await pool.end();
      }
    });
  });

  describe("runContract", () => {
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

    it("returns success with 0 ops when no expanded records exist", async () => {
      const config = resolveConfig({
        connectionString,
        pgSchema: "public",
      });

      const result = await runContract(config);
      expect(result.success).toBe(true);
      expect(result.operationsExecuted).toBe(0);
    });
  });

  describe("showExpandStatus", () => {
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

    it("runs without error when no records exist", async () => {
      const config = resolveConfig({
        connectionString,
        pgSchema: "public",
      });

      await showExpandStatus(config);
    });
  });
});
