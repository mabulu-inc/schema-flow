// test/scaffold.test.ts
// Integration tests for scaffolding and generate-from-db

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { existsSync, readFileSync, rmSync } from "node:fs";
import path from "node:path";
import { createTempProject, execSql, useTestProject } from "./helpers.js";
import { resolveConfig } from "../src/core/config.js";
import { scaffoldPre, scaffoldPost, scaffoldInit, generateFromDb } from "../src/scaffold/index.js";
import { logger, LogLevel } from "../src/core/logger.js";
import { closePool } from "../src/core/db.js";

logger.setLevel(LogLevel.SILENT);

describe("scaffold", () => {
  let project: ReturnType<typeof createTempProject>;

  beforeEach(() => {
    project = createTempProject();
  });

  afterEach(async () => {
    await closePool();
    project.cleanup();
  });

  describe("scaffoldInit", () => {
    it("creates schema, pre, post, and mixins directories", async () => {
      const baseDir = path.join("/tmp", `sf_init_test_${Date.now()}`);
      try {
        scaffoldInit(baseDir);
        expect(existsSync(path.join(baseDir, "schema-flow", "schema"))).toBe(true);
        expect(existsSync(path.join(baseDir, "schema-flow", "pre"))).toBe(true);
        expect(existsSync(path.join(baseDir, "schema-flow", "post"))).toBe(true);
        expect(existsSync(path.join(baseDir, "schema-flow", "mixins"))).toBe(true);
        expect(existsSync(path.join(baseDir, "schema-flow", "schema", ".gitkeep"))).toBe(true);
        expect(existsSync(path.join(baseDir, "schema-flow", "mixins", ".gitkeep"))).toBe(true);
      } finally {
        rmSync(baseDir, { recursive: true, force: true });
      }
    });
  });

  describe("scaffoldPre", () => {
    it("creates a timestamped pre-migration script", () => {
      const config = resolveConfig({
        connectionString: "not-needed",
        baseDir: project.baseDir,
      });

      const filePath = scaffoldPre(config, "add_audit_columns");
      expect(existsSync(filePath)).toBe(true);

      const content = readFileSync(filePath, "utf-8");
      expect(content).toContain("Pre-migration script");
      expect(content).toContain("BEGIN");
      expect(content).toContain("COMMIT");

      const filename = path.basename(filePath);
      expect(filename).toMatch(/^\d{14}_add_audit_columns\.sql$/);
    });
  });

  describe("scaffoldPost", () => {
    it("creates a timestamped post-migration script", () => {
      const config = resolveConfig({
        connectionString: "not-needed",
        baseDir: project.baseDir,
      });

      const filePath = scaffoldPost(config, "seed_data");
      expect(existsSync(filePath)).toBe(true);

      const content = readFileSync(filePath, "utf-8");
      expect(content).toContain("Post-migration script");

      const filename = path.basename(filePath);
      expect(filename).toMatch(/^\d{14}_seed_data\.sql$/);
    });
  });
});

describe("generateFromDb", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("generates YAML files from existing tables", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE users (
        id serial PRIMARY KEY,
        email varchar(255) NOT NULL UNIQUE,
        bio text,
        active boolean NOT NULL DEFAULT true
      )`,
    );

    await execSql(
      ctx.connectionString,
      `CREATE TABLE posts (
        id serial PRIMARY KEY,
        user_id integer REFERENCES users(id) ON DELETE CASCADE,
        title varchar(200) NOT NULL,
        created_at timestamptz NOT NULL DEFAULT now()
      )`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
    });

    const files = await generateFromDb(config);
    expect(files.length).toBeGreaterThanOrEqual(2);

    const usersFile = path.join(ctx.project.schemaDir, "users.yaml");
    const postsFile = path.join(ctx.project.schemaDir, "posts.yaml");

    expect(existsSync(usersFile)).toBe(true);
    expect(existsSync(postsFile)).toBe(true);

    const usersContent = readFileSync(usersFile, "utf-8");
    expect(usersContent).toContain("table: users");
    expect(usersContent).toContain("email");
    expect(usersContent).toContain("varchar");

    const postsContent = readFileSync(postsFile, "utf-8");
    expect(postsContent).toContain("table: posts");
    expect(postsContent).toContain("references");
    expect(postsContent).toContain("users");
  });

  it("generates YAML files with triggers", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE triggered (
        id serial PRIMARY KEY,
        updated_at timestamptz NOT NULL DEFAULT now()
      )`,
    );
    await execSql(
      ctx.connectionString,
      `CREATE OR REPLACE FUNCTION update_timestamp() RETURNS trigger LANGUAGE plpgsql AS $$
       BEGIN NEW.updated_at = now(); RETURN NEW; END; $$`,
    );
    await execSql(
      ctx.connectionString,
      `CREATE TRIGGER set_triggered_updated BEFORE UPDATE ON triggered
       FOR EACH ROW EXECUTE FUNCTION update_timestamp()`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
    });

    await generateFromDb(config);
    const triggeredFile = path.join(ctx.project.schemaDir, "triggered.yaml");
    expect(existsSync(triggeredFile)).toBe(true);

    const content = readFileSync(triggeredFile, "utf-8");
    expect(content).toContain("triggers");
    expect(content).toContain("set_triggered_updated");
    expect(content).toContain("update_timestamp");
  });

  it("excludes the schema-flow history table", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE _schema_flow_history (
        file_path text PRIMARY KEY,
        file_hash text NOT NULL,
        phase text NOT NULL,
        applied_at timestamptz DEFAULT now()
      )`,
    );

    await execSql(ctx.connectionString, `CREATE TABLE real_table (id serial PRIMARY KEY)`);

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
    });

    const files = await generateFromDb(config);
    const filenames = files.map((f) => path.basename(f));
    expect(filenames).not.toContain("_schema_flow_history.yaml");
    expect(filenames).toContain("real_table.yaml");
  });

  it("preserves TABLE return type for table-returning functions", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE OR REPLACE FUNCTION session_authorization()
       RETURNS TABLE(sa_tenant_id uuid, sa_producer_id uuid, sa_region_id uuid, sa_plant_id uuid)
       LANGUAGE plpgsql SECURITY DEFINER AS $$
       BEGIN
         RETURN QUERY SELECT NULL::uuid, NULL::uuid, NULL::uuid, NULL::uuid;
       END;
       $$`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
    });

    await generateFromDb(config);

    const fnFile = path.join(ctx.project.schemaDir, "fn_session_authorization.yaml");
    expect(existsSync(fnFile)).toBe(true);

    const content = readFileSync(fnFile, "utf-8");
    expect(content).toContain("TABLE(sa_tenant_id uuid, sa_producer_id uuid, sa_region_id uuid, sa_plant_id uuid)");
    expect(content).not.toMatch(/returns: record\b/);
  });

  it("generates SETOF return type for set-returning functions", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE sessions (
        session_id text PRIMARY KEY,
        user_id integer NOT NULL
      )`,
    );

    await execSql(
      ctx.connectionString,
      `CREATE OR REPLACE FUNCTION get_active_sessions()
       RETURNS SETOF record LANGUAGE plpgsql SECURITY DEFINER AS $$
       BEGIN
         RETURN QUERY SELECT session_id, user_id FROM sessions;
       END;
       $$`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
    });

    await generateFromDb(config);

    const fnFile = path.join(ctx.project.schemaDir, "fn_get_active_sessions.yaml");
    expect(existsSync(fnFile)).toBe(true);

    const content = readFileSync(fnFile, "utf-8");
    expect(content).toContain("SETOF record");
    expect(content).not.toMatch(/returns: record\b/);
  });
});
