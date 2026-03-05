// test/check-normalization.test.ts
// Verifies that CHECK constraint expressions survive a generate → run → drift round-trip
// without producing false drift. PG may re-normalize expressions when storing them,
// so the expression read back from the target DB must match the YAML produced by generate.

import { describe, it, expect } from "vitest";
import { createTestDb, execSql, createTempProject } from "./helpers.js";
import { resolveConfig } from "../src/core/config.js";
import { closePool } from "../src/core/db.js";
import { generateFromDb } from "../src/scaffold/index.js";
import { runMigrate } from "../src/executor/index.js";
import { detectDrift } from "../src/drift/index.js";
import { logger, LogLevel } from "../src/core/logger.js";

logger.setLevel(LogLevel.SILENT);

describe("check expression round-trip (generate → run → drift)", () => {
  let sourceDb: { connectionString: string; cleanup: () => Promise<void> };
  let targetDb: { connectionString: string; cleanup: () => Promise<void> };
  let project: ReturnType<typeof createTempProject>;

  afterEach(async () => {
    await closePool();
    await sourceDb?.cleanup();
    await targetDb?.cleanup();
    project?.cleanup();
  });

  async function roundTrip(setupSql: string) {
    // 1. Create source DB with the schema
    sourceDb = await createTestDb();
    await execSql(sourceDb.connectionString, setupSql);

    // 2. Generate YAML from source DB
    project = createTempProject();
    const sourceConfig = resolveConfig({
      connectionString: sourceDb.connectionString,
      baseDir: project.baseDir,
    });
    await generateFromDb(sourceConfig);

    // 3. Create target DB (empty) and run the generated YAML into it
    targetDb = await createTestDb();
    const targetConfig = resolveConfig({
      connectionString: targetDb.connectionString,
      baseDir: project.baseDir,
    });
    await closePool(); // close source pool before switching
    const result = await runMigrate(targetConfig);
    expect(result.success).toBe(true);

    // 4. Drift detection on target should be clean
    await closePool();
    const report = await detectDrift(targetConfig);
    return report;
  }

  it("simple check expression", async () => {
    const report = await roundTrip(`
      CREATE TABLE products (
        id serial PRIMARY KEY,
        price integer NOT NULL,
        CONSTRAINT chk_positive_price CHECK (price > 0)
      )
    `);
    const checkDrift = report.items.filter((i) => i.category === "constraint" && i.name === "chk_positive_price");
    expect(checkDrift).toHaveLength(0);
  });

  it("check with IN list / ANY array", async () => {
    const report = await roundTrip(`
      CREATE TABLE events (
        id serial PRIMARY KEY,
        source varchar(50) NOT NULL,
        CONSTRAINT chk_source CHECK (source = ANY(ARRAY['web', 'mobile', 'api']::text[]))
      )
    `);
    const checkDrift = report.items.filter((i) => i.category === "constraint" && i.name === "chk_source");
    expect(checkDrift).toHaveLength(0);
  });

  it("check with varchar array cast (element-distributed)", async () => {
    const report = await roundTrip(`
      CREATE TABLE entries (
        id serial PRIMARY KEY,
        status character varying(20) NOT NULL,
        CONSTRAINT chk_status CHECK (status::text = ANY(ARRAY['active'::character varying, 'inactive'::character varying, 'pending'::character varying]::text[]))
      )
    `);
    const checkDrift = report.items.filter((i) => i.category === "constraint" && i.name === "chk_status");
    expect(checkDrift).toHaveLength(0);
  });

  it("check with array-level cast that PG re-normalizes on create", async () => {
    // When the original DDL uses ARRAY[...]::varchar[], PG stores the array-level
    // cast form: (ARRAY[...]::text[]). But when that expression is used to CREATE
    // a new constraint, PG re-normalizes it by distributing the cast to each element:
    // ARRAY[(...)::text, ...]. This causes false drift.
    const report = await roundTrip(`
      CREATE TABLE events (
        id serial PRIMARY KEY,
        source varchar(50) NOT NULL,
        CONSTRAINT chk_source CHECK (source = ANY(ARRAY['web', 'mobile', 'api']::varchar[]))
      )
    `);
    const checkDrift = report.items.filter((i) => i.category === "constraint" && i.name === "chk_source");
    expect(checkDrift).toHaveLength(0);
  });

  it("check with boolean logic", async () => {
    const report = await roundTrip(`
      CREATE TABLE ranges (
        id serial PRIMARY KEY,
        low integer NOT NULL,
        high integer NOT NULL,
        CONSTRAINT chk_range CHECK (low >= 0 AND high > low AND high <= 1000)
      )
    `);
    const checkDrift = report.items.filter((i) => i.category === "constraint" && i.name === "chk_range");
    expect(checkDrift).toHaveLength(0);
  });

  it("check with type cast on column", async () => {
    const report = await roundTrip(`
      CREATE TABLE tagged (
        id serial PRIMARY KEY,
        tags text[] NOT NULL,
        CONSTRAINT chk_tags CHECK (array_length(tags, 1) <= 10)
      )
    `);
    const checkDrift = report.items.filter((i) => i.category === "constraint" && i.name === "chk_tags");
    expect(checkDrift).toHaveLength(0);
  });
});
