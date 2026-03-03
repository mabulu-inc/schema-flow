// test/drift.test.ts
// Tests for drift detection

import { describe, it, expect } from "vitest";
import { useTestProject, writeSchema, execSql } from "./helpers.js";
import { resolveConfig } from "../src/core/config.js";
import { closePool } from "../src/core/db.js";
import { logger, LogLevel } from "../src/core/logger.js";
import { detectDrift } from "../src/drift/index.js";
import { formatDriftReport, formatDriftReportJson } from "../src/drift/format.js";

logger.setLevel(LogLevel.SILENT);

describe("drift detection", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("reports no drift when DB matches YAML", async () => {
    await execSql(ctx.connectionString, `CREATE TABLE users (id serial PRIMARY KEY, email varchar(255) NOT NULL)`);

    writeSchema(
      ctx.project.schemaDir,
      "users.yaml",
      `table: users
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: varchar(255)
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
    });

    const report = await detectDrift(config);
    expect(report.hasDrift).toBe(false);
    expect(report.items).toHaveLength(0);
    expect(report.summary.tablesChecked).toBe(1);
  });

  it("detects table missing from DB", async () => {
    writeSchema(
      ctx.project.schemaDir,
      "users.yaml",
      `table: users
columns:
  - name: id
    type: serial
    primary_key: true
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
    });

    const report = await detectDrift(config);
    expect(report.hasDrift).toBe(true);
    const missing = report.items.find((i) => i.direction === "missing_from_db" && i.category === "table");
    expect(missing).toBeDefined();
    expect(missing!.name).toBe("users");
  });

  it("detects extra table in DB", async () => {
    await execSql(ctx.connectionString, `CREATE TABLE orphan (id serial PRIMARY KEY)`);

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
    });

    const report = await detectDrift(config);
    expect(report.hasDrift).toBe(true);
    const extra = report.items.find((i) => i.direction === "extra_in_db" && i.name === "orphan");
    expect(extra).toBeDefined();
  });

  it("detects missing column in DB", async () => {
    await execSql(ctx.connectionString, `CREATE TABLE users (id serial PRIMARY KEY)`);

    writeSchema(
      ctx.project.schemaDir,
      "users.yaml",
      `table: users
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: varchar(255)
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
    });

    const report = await detectDrift(config);
    expect(report.hasDrift).toBe(true);
    const missing = report.items.find((i) => i.category === "column" && i.direction === "missing_from_db");
    expect(missing).toBeDefined();
    expect(missing!.name).toBe("email");
  });

  it("detects extra column in DB", async () => {
    await execSql(ctx.connectionString, `CREATE TABLE users (id serial PRIMARY KEY, obsolete text)`);

    writeSchema(
      ctx.project.schemaDir,
      "users.yaml",
      `table: users
columns:
  - name: id
    type: serial
    primary_key: true
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
    });

    const report = await detectDrift(config);
    expect(report.hasDrift).toBe(true);
    const extra = report.items.find((i) => i.category === "column" && i.direction === "extra_in_db");
    expect(extra).toBeDefined();
    expect(extra!.name).toBe("obsolete");
  });

  it("detects nullable mismatch", async () => {
    await execSql(ctx.connectionString, `CREATE TABLE users (id serial PRIMARY KEY, name text)`);

    writeSchema(
      ctx.project.schemaDir,
      "users.yaml",
      `table: users
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
    });

    const report = await detectDrift(config);
    expect(report.hasDrift).toBe(true);
    const mismatch = report.items.find((i) => i.category === "column" && i.direction === "mismatch");
    expect(mismatch).toBeDefined();
    expect(mismatch!.details?.some((d) => d.field === "nullable")).toBe(true);
  });

  it("formats text report correctly", async () => {
    await execSql(ctx.connectionString, `CREATE TABLE users (id serial PRIMARY KEY)`);

    writeSchema(
      ctx.project.schemaDir,
      "users.yaml",
      `table: users
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: varchar(255)
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
    });

    const report = await detectDrift(config);
    const text = formatDriftReport(report);
    expect(text).toContain("Drift detected");
    expect(text).toContain("MISSING FROM DB");
    expect(text).toContain("email");
  });

  it("formats JSON report", async () => {
    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
    });

    const report = await detectDrift(config);
    const json = formatDriftReportJson(report);
    const parsed = JSON.parse(json);
    expect(parsed).toHaveProperty("items");
    expect(parsed).toHaveProperty("hasDrift");
    expect(parsed).toHaveProperty("summary");
  });
});
