// test/smoke-test.test.ts
// Tests for function smoke testing after creation/modification

import { describe, it, expect } from "vitest";
import { writeSchema, useTestProject, execSql } from "./helpers.js";
import { resolveConfig } from "../src/core/config.js";
import { runMigrate, runValidate } from "../src/executor/index.js";
import { closePool } from "../src/core/db.js";
import { logger, LogLevel } from "../src/core/logger.js";

logger.setLevel(LogLevel.SILENT);

describe("function smoke tests", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("catches a function with dynamic SQL referencing a non-existent table", async () => {
    // The function body uses EXECUTE (dynamic SQL), so PG won't validate
    // the table reference at CREATE FUNCTION time. The smoke test should
    // catch this at migration time.
    writeSchema(
      ctx.project.functionsDir,
      "bad_lookup.yaml",
      `name: bad_lookup
language: plpgsql
returns: text
args: "p_id integer"
replace: true
body: |
  DECLARE
    v_result text;
  BEGIN
    SELECT name INTO v_result FROM completely_nonexistent_table WHERE id = p_id;
    RETURN v_result;
  END;
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    const result = await runMigrate(config);
    expect(result.success).toBe(false);
    expect(result.errors.join(" ")).toMatch(/completely_nonexistent_table/);
  });

  it("passes smoke test for a valid function", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "items.yaml",
      `table: items
columns:
  - name: id
    type: serial
    primary_key: true
  - name: label
    type: text
`,
    );
    writeSchema(
      ctx.project.functionsDir,
      "get_label.yaml",
      `name: get_label
language: plpgsql
returns: text
args: "p_id integer"
replace: true
body: |
  DECLARE
    v_label text;
  BEGIN
    SELECT label INTO v_label FROM items WHERE id = p_id;
    RETURN v_label;
  END;
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    const result = await runMigrate(config);
    expect(result.success).toBe(true);
  });

  it("skips smoke test for trigger functions", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "posts.yaml",
      `table: posts
columns:
  - name: id
    type: serial
    primary_key: true
  - name: updated_at
    type: timestamptz
    default: now()
`,
    );
    writeSchema(
      ctx.project.functionsDir,
      "set_updated_at.yaml",
      `name: set_updated_at
language: plpgsql
returns: trigger
replace: true
body: |
  BEGIN
    NEW.updated_at = now();
    RETURN NEW;
  END;
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    const result = await runMigrate(config);
    expect(result.success).toBe(true);
  });

  it("validate command also smoke-tests functions", async () => {
    writeSchema(
      ctx.project.functionsDir,
      "bad_validate.yaml",
      `name: bad_validate
language: plpgsql
returns: void
replace: true
body: |
  BEGIN
    INSERT INTO table_that_does_not_exist (col) VALUES ('x');
  END;
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    const result = await runValidate(config);
    expect(result.valid).toBe(false);
    expect(result.errors.join(" ")).toMatch(/table_that_does_not_exist/);
  });

  it("smoke test does not leave side effects for valid functions", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "counters.yaml",
      `table: counters
columns:
  - name: id
    type: serial
    primary_key: true
  - name: val
    type: integer
    default: "0"
`,
    );
    writeSchema(
      ctx.project.functionsDir,
      "bump_counter.yaml",
      `name: bump_counter
language: plpgsql
returns: void
args: "p_id integer"
replace: true
body: |
  BEGIN
    UPDATE counters SET val = val + 1 WHERE id = p_id;
  END;
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    // No rows should have been inserted/modified by the smoke test
    const res = await execSql(ctx.connectionString, "SELECT count(*) AS n FROM counters");
    expect(res.rows[0].n).toBe("0");
  });
});
