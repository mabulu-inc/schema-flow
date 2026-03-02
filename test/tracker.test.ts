// test/tracker.test.ts
// Integration tests for file hash tracking

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { writeFileSync } from "node:fs";
import { createTempProject, useTestClient } from "./helpers.js";
import { FileTracker } from "../src/core/tracker.js";

describe("FileTracker", () => {
  const ctx = useTestClient();
  let project: ReturnType<typeof createTempProject>;

  beforeEach(() => {
    project = createTempProject();
  });

  afterEach(() => {
    project?.cleanup();
  });

  it("creates the tracking table on first use", async () => {
    const tracker = new FileTracker("_schema_flow_history");
    await tracker.ensureTable(ctx.client);

    const res = await ctx.client.query(
      `SELECT 1 FROM information_schema.tables
       WHERE table_name = '_schema_flow_history'`,
    );
    expect(res.rowCount).toBe(1);
  });

  it("classifies new files correctly", async () => {
    const tracker = new FileTracker("_schema_flow_history");
    await tracker.ensureTable(ctx.client);
    const tracked = await tracker.getTracked(ctx.client);

    const filePath = `${project.schemaDir}/users.yaml`;
    writeFileSync(filePath, "table: users\ncolumns: []", "utf-8");

    const result = tracker.classifyFiles([filePath], tracked, "schema");
    expect(result.newFiles).toHaveLength(1);
    expect(result.changedFiles).toHaveLength(0);
    expect(result.unchangedFiles).toHaveLength(0);
  });

  it("classifies unchanged files correctly after recording", async () => {
    const tracker = new FileTracker("_schema_flow_history");
    await tracker.ensureTable(ctx.client);

    const filePath = `${project.schemaDir}/users.yaml`;
    writeFileSync(filePath, "table: users\ncolumns: []", "utf-8");

    await tracker.recordFile(ctx.client, filePath, "schema");
    const tracked = await tracker.getTracked(ctx.client);
    const result = tracker.classifyFiles([filePath], tracked, "schema");

    expect(result.newFiles).toHaveLength(0);
    expect(result.changedFiles).toHaveLength(0);
    expect(result.unchangedFiles).toHaveLength(1);
  });

  it("detects changed files when content is modified", async () => {
    const tracker = new FileTracker("_schema_flow_history");
    await tracker.ensureTable(ctx.client);

    const filePath = `${project.schemaDir}/users.yaml`;
    writeFileSync(filePath, "table: users\ncolumns: []", "utf-8");
    await tracker.recordFile(ctx.client, filePath, "schema");

    // Modify the file
    writeFileSync(filePath, "table: users\ncolumns:\n  - name: id\n    type: serial", "utf-8");

    const tracked = await tracker.getTracked(ctx.client);
    const result = tracker.classifyFiles([filePath], tracked, "schema");

    expect(result.newFiles).toHaveLength(0);
    expect(result.changedFiles).toHaveLength(1);
    expect(result.unchangedFiles).toHaveLength(0);
  });

  it("updates hash when recording a changed file", async () => {
    const tracker = new FileTracker("_schema_flow_history");
    await tracker.ensureTable(ctx.client);

    const filePath = `${project.schemaDir}/users.yaml`;
    writeFileSync(filePath, "v1", "utf-8");
    await tracker.recordFile(ctx.client, filePath, "schema");

    writeFileSync(filePath, "v2", "utf-8");
    await tracker.recordFile(ctx.client, filePath, "schema");

    const tracked = await tracker.getTracked(ctx.client);
    const result = tracker.classifyFiles([filePath], tracked, "schema");

    expect(result.unchangedFiles).toHaveLength(1);
    expect(result.changedFiles).toHaveLength(0);
  });
});
