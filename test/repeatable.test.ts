import { describe, it, expect } from "vitest";
import { writeFileSync, mkdirSync } from "node:fs";
import path from "node:path";
import { useTestProject, writeSchema, execSql } from "./helpers.js";
import { runRepeatables, runMigrate } from "../src/executor/index.js";
import { resolveConfig } from "../src/core/config.js";
import { closePool } from "../src/core/db.js";

describe("repeatable migrations", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  function writeRepeatable(baseDir: string, filename: string, content: string): string {
    const repeatableDir = path.join(baseDir, "schema-flow", "repeatable");
    mkdirSync(repeatableDir, { recursive: true });
    const filePath = path.join(repeatableDir, filename);
    writeFileSync(filePath, content, "utf-8");
    return filePath;
  }

  it("executes new repeatable SQL files", async () => {
    // Create a table first so the grant has something to target
    writeSchema(ctx.project.schemaDir, "items.yaml", `
table: items
columns:
  - name: id
    type: serial
    primary_key: true
`);

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
    });

    // Run migrate first to create the table
    await runMigrate(config);

    // Write a repeatable script
    writeRepeatable(ctx.project.baseDir, "create_test_table.sql", `
      CREATE TABLE IF NOT EXISTS repeatable_test (id serial PRIMARY KEY);
    `);

    const result = await runRepeatables(config);
    expect(result.success).toBe(true);
    expect(result.filesExecuted).toBe(1);

    // Verify the table was created
    const res = await execSql(ctx.connectionString,
      `SELECT 1 FROM information_schema.tables WHERE table_name = 'repeatable_test'`
    );
    expect(res.rowCount).toBe(1);
  });

  it("skips unchanged repeatable files", async () => {
    writeRepeatable(ctx.project.baseDir, "idempotent.sql", `
      CREATE TABLE IF NOT EXISTS rep_skip (id serial PRIMARY KEY);
    `);

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
    });

    // Run once
    await runRepeatables(config);

    // Run again — should skip
    const result = await runRepeatables(config);
    expect(result.success).toBe(true);
    expect(result.filesExecuted).toBe(0);
  });

  it("returns success when no repeatable directory exists", async () => {
    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
    });

    const result = await runRepeatables(config);
    expect(result.success).toBe(true);
    expect(result.filesExecuted).toBe(0);
  });
});
