// Integration test: multi-schema
// Demonstrates running schema-flow against two PostgreSQL schemas (core, analytics)
// where analytics tables have cross-schema FKs into core.

import { describe, it, expect, beforeAll, afterAll } from "vitest";
import path from "node:path";
import { resolveConfig } from "../../src/core/config.js";
import { runMigrate } from "../../src/executor/index.js";
import { closePool } from "../../src/core/db.js";
import { logger, LogLevel } from "../../src/core/logger.js";
import { createTestDb, execSql } from "../../src/testing/index.js";

logger.setLevel(LogLevel.SILENT);

describe("example: multi-schema", () => {
  let db: string;
  let cleanup: () => Promise<void>;

  beforeAll(async () => {
    const testDb = await createTestDb();
    db = testDb.connectionString;
    cleanup = testDb.cleanup;

    // Create the two schemas up front
    await execSql(db, `CREATE SCHEMA IF NOT EXISTS core`);
    await execSql(db, `CREATE SCHEMA IF NOT EXISTS analytics`);

    // 1. Migrate core first — analytics depends on it
    const coreConfig = resolveConfig({
      connectionString: db,
      baseDir: path.resolve(__dirname, "core"),
      pgSchema: "core",
      dryRun: false,
    });
    const coreResult = await runMigrate(coreConfig);
    expect(coreResult.success).toBe(true);
    await closePool();

    // 2. Migrate analytics second — cross-schema FKs point to core
    const analyticsConfig = resolveConfig({
      connectionString: db,
      baseDir: path.resolve(__dirname, "analytics"),
      pgSchema: "analytics",
      dryRun: false,
    });
    const analyticsResult = await runMigrate(analyticsConfig);
    expect(analyticsResult.success).toBe(true);
  });

  afterAll(async () => {
    await closePool();
    await cleanup();
  });

  it("creates tables in both schemas", async () => {
    const core = await execSql(
      db,
      `SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'core' ORDER BY table_name`,
    );
    expect(core.rows.map((r: { table_name: string }) => r.table_name)).toContain("users");
    expect(core.rows.map((r: { table_name: string }) => r.table_name)).toContain("teams");

    const analytics = await execSql(
      db,
      `SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'analytics' ORDER BY table_name`,
    );
    expect(analytics.rows.map((r: { table_name: string }) => r.table_name)).toContain("events");
    expect(analytics.rows.map((r: { table_name: string }) => r.table_name)).toContain("dashboards");
  });

  it("enforces cross-schema FK from analytics.events to core.users", async () => {
    const team = await execSql(db, `INSERT INTO core.teams (name) VALUES ('Eng') RETURNING id`);
    const user = await execSql(
      db,
      `INSERT INTO core.users (email, display_name, team_id)
       VALUES ('alice@example.com', 'Alice', $1) RETURNING id`,
      [team.rows[0].id],
    );

    // Valid FK — should succeed
    await execSql(
      db,
      `INSERT INTO analytics.events (user_id, event_type, payload)
       VALUES ($1, 'page_view', '{"page": "/home"}')`,
      [user.rows[0].id],
    );

    // Invalid FK — should fail
    await expect(
      execSql(db, `INSERT INTO analytics.events (user_id, event_type) VALUES (99999, 'bogus')`),
    ).rejects.toThrow();
  });

  it("enforces cross-schema FK from analytics.dashboards to core.users and core.teams", async () => {
    const user = await execSql(db, `SELECT id FROM core.users LIMIT 1`);
    const team = await execSql(db, `SELECT id FROM core.teams LIMIT 1`);

    const dash = await execSql(
      db,
      `INSERT INTO analytics.dashboards (title, owner_id, team_id)
       VALUES ('KPIs', $1, $2) RETURNING *`,
      [user.rows[0].id, team.rows[0].id],
    );
    expect(dash.rows[0].title).toBe("KPIs");

    // team_id is nullable — should work with NULL
    const personal = await execSql(
      db,
      `INSERT INTO analytics.dashboards (title, owner_id)
       VALUES ('Personal', $1) RETURNING team_id`,
      [user.rows[0].id],
    );
    expect(personal.rows[0].team_id).toBeNull();
  });

  it("recent_events view joins across schemas", async () => {
    const res = await execSql(db, `SELECT * FROM analytics.recent_events LIMIT 5`);
    expect(res.rows.length).toBeGreaterThan(0);
    expect(res.rows[0]).toHaveProperty("user_email");
    expect(res.rows[0]).toHaveProperty("event_type");
  });

  it("second migration is a no-op for both schemas", async () => {
    await closePool();

    const coreConfig = resolveConfig({
      connectionString: db,
      baseDir: path.resolve(__dirname, "core"),
      pgSchema: "core",
      dryRun: false,
    });
    const coreResult = await runMigrate(coreConfig);
    expect(coreResult.success).toBe(true);
    expect(coreResult.operationsExecuted).toBe(0);
    await closePool();

    const analyticsConfig = resolveConfig({
      connectionString: db,
      baseDir: path.resolve(__dirname, "analytics"),
      pgSchema: "analytics",
      dryRun: false,
    });
    const analyticsResult = await runMigrate(analyticsConfig);
    expect(analyticsResult.success).toBe(true);
    expect(analyticsResult.operationsExecuted).toBe(0);
  });
});
