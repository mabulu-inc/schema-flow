// Integration test: todo-app
// Verifies application behavior — can users manage todos and categories?
//
// Standalone usage (outside this repo):
//   import { resolveConfig, runMigrate, closePool, ... } from "@mabulu-inc/schema-flow";
//   import { createTestDb, execSql } from "@mabulu-inc/schema-flow/testing";

import { describe, it, expect, beforeAll, afterAll } from "vitest";
import path from "node:path";
import { resolveConfig } from "../../src/core/config.js";
import { runMigrate } from "../../src/executor/index.js";
import { closePool } from "../../src/core/db.js";
import { logger, LogLevel } from "../../src/core/logger.js";
import { createTestDb, execSql } from "../../src/testing/index.js";

logger.setLevel(LogLevel.SILENT);

describe("example: todo-app", () => {
  let db: string;
  let cleanup: () => Promise<void>;

  beforeAll(async () => {
    const testDb = await createTestDb();
    db = testDb.connectionString;
    cleanup = testDb.cleanup;

    const config = resolveConfig({
      connectionString: db,
      baseDir: path.resolve(__dirname),
      dryRun: false,
    });
    const result = await runMigrate(config);
    expect(result.success).toBe(true);
  });

  afterAll(async () => {
    await closePool();
    await cleanup();
  });

  it("creates a category and a todo in that category", async () => {
    const cat = await execSql(db, `INSERT INTO categories (name, color) VALUES ('Work', '#3B82F6') RETURNING id`);
    const todo = await execSql(
      db,
      `INSERT INTO todos (title, category_id, priority) VALUES ('Finish report', $1, 'high') RETURNING *`,
      [cat.rows[0].id],
    );

    expect(todo.rows[0].title).toBe("Finish report");
    expect(todo.rows[0].priority).toBe("high");
    expect(todo.rows[0].category_id).toBe(cat.rows[0].id);
  });

  it("defaults priority to medium", async () => {
    const res = await execSql(db, `INSERT INTO todos (title) VALUES ('No priority set') RETURNING priority`);
    expect(res.rows[0].priority).toBe("medium");
  });

  it("rejects empty titles", async () => {
    await expect(execSql(db, `INSERT INTO todos (title) VALUES ('  ')`)).rejects.toThrow();
  });

  it("rejects invalid color codes", async () => {
    await expect(execSql(db, `INSERT INTO categories (name, color) VALUES ('Bad', 'red')`)).rejects.toThrow();
  });

  it("nullifies category_id when category is deleted", async () => {
    const cat = await execSql(db, `INSERT INTO categories (name) VALUES ('Temp') RETURNING id`);
    await execSql(db, `INSERT INTO todos (title, category_id) VALUES ('Orphan', $1)`, [cat.rows[0].id]);

    await execSql(db, `DELETE FROM categories WHERE id = $1`, [cat.rows[0].id]);

    const res = await execSql(db, `SELECT category_id FROM todos WHERE title = 'Orphan'`);
    expect(res.rows[0].category_id).toBeNull();
  });

  it("auto-updates updated_at on modification", async () => {
    await execSql(db, `INSERT INTO todos (title) VALUES ('Will update')`);
    const before = await execSql(db, `SELECT updated_at FROM todos WHERE title = 'Will update'`);

    await new Promise((r) => setTimeout(r, 50));
    await execSql(db, `UPDATE todos SET title = 'Updated' WHERE title = 'Will update'`);

    const after = await execSql(db, `SELECT updated_at FROM todos WHERE title = 'Updated'`);
    expect(new Date(after.rows[0].updated_at).getTime()).toBeGreaterThan(new Date(before.rows[0].updated_at).getTime());
  });

  it("marks a todo as completed", async () => {
    await execSql(db, `INSERT INTO todos (title) VALUES ('Complete me')`);
    await execSql(db, `UPDATE todos SET completed_at = now() WHERE title = 'Complete me'`);

    const res = await execSql(db, `SELECT completed_at FROM todos WHERE title = 'Complete me'`);
    expect(res.rows[0].completed_at).not.toBeNull();
  });

  it("second migration is a no-op", async () => {
    await closePool();
    const config = resolveConfig({
      connectionString: db,
      baseDir: path.resolve(__dirname),
      dryRun: false,
    });
    const result = await runMigrate(config);
    expect(result.success).toBe(true);
    expect(result.operationsExecuted).toBe(0);
  });
});
