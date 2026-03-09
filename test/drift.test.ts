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
      ctx.project.tablesDir,
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
      ctx.project.tablesDir,
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
      ctx.project.tablesDir,
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
      ctx.project.tablesDir,
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
      ctx.project.tablesDir,
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
      ctx.project.tablesDir,
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

  it("detects function return type mismatch", async () => {
    // Create function with TABLE return type in DB
    await execSql(
      ctx.connectionString,
      `CREATE OR REPLACE FUNCTION session_authorization()
       RETURNS TABLE(sa_tenant_id uuid, sa_producer_id uuid)
       LANGUAGE plpgsql SECURITY DEFINER AS $$
       BEGIN
         RETURN QUERY SELECT NULL::uuid, NULL::uuid;
       END;
       $$`,
    );

    // Write YAML with wrong return type (record instead of TABLE)
    writeSchema(
      ctx.project.functionsDir,
      "session_authorization.yaml",
      `name: session_authorization
language: plpgsql
returns: record
body: |
  BEGIN
    RETURN QUERY SELECT NULL::uuid, NULL::uuid;
  END;
replace: true
security: definer
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
    });

    const report = await detectDrift(config);
    const fnDrift = report.items.filter((i) => i.category === "function" && i.name === "session_authorization");
    expect(fnDrift).toHaveLength(1);
    expect(fnDrift[0].direction).toBe("mismatch");
    expect(fnDrift[0].details).toBeDefined();
    expect(fnDrift[0].details!.some((d) => d.field === "returns")).toBe(true);
  });

  it("detects function body mismatch", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE OR REPLACE FUNCTION greet()
       RETURNS text LANGUAGE plpgsql AS $$
       BEGIN
         RETURN 'hello';
       END;
       $$`,
    );

    writeSchema(
      ctx.project.functionsDir,
      "greet.yaml",
      `name: greet
language: plpgsql
returns: text
body: |
  BEGIN
    RETURN 'goodbye';
  END;
replace: true
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
    });

    const report = await detectDrift(config);
    const fnDrift = report.items.filter((i) => i.category === "function" && i.name === "greet");
    expect(fnDrift).toHaveLength(1);
    expect(fnDrift[0].direction).toBe("mismatch");
    expect(fnDrift[0].details!.some((d) => d.field === "body")).toBe(true);
  });

  it("no drift when function body matches", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE OR REPLACE FUNCTION add_one(x integer)
       RETURNS integer LANGUAGE plpgsql AS $$
       BEGIN
         RETURN x + 1;
       END;
       $$`,
    );

    writeSchema(
      ctx.project.functionsDir,
      "add_one.yaml",
      `name: add_one
language: plpgsql
returns: integer
args:
  - name: x
    type: integer
body: |
  BEGIN
    RETURN x + 1;
  END;
replace: true
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
    });

    const report = await detectDrift(config);
    const fnDrift = report.items.filter(
      (i) => i.category === "function" && i.name === "add_one" && i.direction === "mismatch",
    );
    expect(fnDrift).toHaveLength(0);
  });

  it("detects missing seed row", async () => {
    await execSql(ctx.connectionString, `CREATE TABLE currencies (code text PRIMARY KEY, name text NOT NULL)`);
    await execSql(ctx.connectionString, `INSERT INTO currencies (code, name) VALUES ('USD', 'US Dollar')`);

    writeSchema(
      ctx.project.tablesDir,
      "currencies.yaml",
      `table: currencies
columns:
  - name: code
    type: text
    primary_key: true
  - name: name
    type: text
seeds:
  - code: USD
    name: US Dollar
  - code: EUR
    name: Euro
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
    });

    const report = await detectDrift(config);
    expect(report.hasDrift).toBe(true);
    const seedDrift = report.items.find((i) => i.category === "seed" && i.table === "currencies");
    expect(seedDrift).toBeDefined();
    expect(seedDrift!.direction).toBe("missing_from_db");
    expect(seedDrift!.description).toContain("EUR");
  });

  it("detects seed row with wrong value", async () => {
    await execSql(ctx.connectionString, `CREATE TABLE currencies (code text PRIMARY KEY, name text NOT NULL)`);
    await execSql(ctx.connectionString, `INSERT INTO currencies (code, name) VALUES ('USD', 'Dollar')`);

    writeSchema(
      ctx.project.tablesDir,
      "currencies.yaml",
      `table: currencies
columns:
  - name: code
    type: text
    primary_key: true
  - name: name
    type: text
seeds:
  - code: USD
    name: US Dollar
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
    });

    const report = await detectDrift(config);
    expect(report.hasDrift).toBe(true);
    const seedDrift = report.items.find((i) => i.category === "seed" && i.table === "currencies");
    expect(seedDrift).toBeDefined();
    expect(seedDrift!.direction).toBe("mismatch");
    expect(seedDrift!.details).toBeDefined();
    expect(
      seedDrift!.details!.some((d) => d.field === "name" && d.expected === "US Dollar" && d.actual === "Dollar"),
    ).toBe(true);
  });

  it("reports no drift when seeds match DB", async () => {
    await execSql(ctx.connectionString, `CREATE TABLE currencies (code text PRIMARY KEY, name text NOT NULL)`);
    await execSql(
      ctx.connectionString,
      `INSERT INTO currencies (code, name) VALUES ('USD', 'US Dollar'), ('EUR', 'Euro')`,
    );

    writeSchema(
      ctx.project.tablesDir,
      "currencies.yaml",
      `table: currencies
columns:
  - name: code
    type: text
    primary_key: true
  - name: name
    type: text
seeds:
  - code: USD
    name: US Dollar
  - code: EUR
    name: Euro
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
    });

    const report = await detectDrift(config);
    const seedDrift = report.items.filter((i) => i.category === "seed");
    expect(seedDrift).toHaveLength(0);
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
