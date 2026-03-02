// test/cli.test.ts
// End-to-end tests for the CLI commands

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { execSync } from "node:child_process";
import { existsSync, readdirSync, rmSync } from "node:fs";
import path from "node:path";
import { createTempProject, writeSchema, tableExists, execSql, getColumns, useTestProject } from "./helpers.js";
import { closePool } from "../src/core/db.js";

const CLI = path.resolve("dist/cli/index.js");

function run(args: string, env: Record<string, string> = {}): string {
  return execSync(`node ${CLI} ${args}`, {
    encoding: "utf-8",
    env: { ...process.env, ...env },
    timeout: 30000,
  });
}

function runSafe(args: string, env: Record<string, string> = {}): { stdout: string; exitCode: number } {
  try {
    const stdout = run(args, env);
    return { stdout, exitCode: 0 };
  } catch (err: unknown) {
    const e = err as { stdout?: string; status?: number };
    return { stdout: e.stdout || "", exitCode: e.status || 1 };
  }
}

describe("CLI", () => {
  describe("help", () => {
    it("prints help with --help flag", () => {
      const output = run("--help");
      expect(output).toContain("schema-flow");
      expect(output).toContain("Commands:");
      expect(output).toContain("run");
      expect(output).toContain("plan");
      expect(output).toContain("generate");
    });

    it("prints help with help command", () => {
      const output = run("help");
      expect(output).toContain("schema-flow");
    });

    it("shows --allow-destructive in help", () => {
      const output = run("--help");
      expect(output).toContain("--allow-destructive");
      expect(output).toContain("Safety:");
    });

    it("mentions mixins in help", () => {
      const output = run("--help");
      expect(output).toContain("mixins");
    });
  });

  describe("init", () => {
    it("creates the directory structure including mixins", () => {
      const baseDir = path.join("/tmp", `sf_cli_init_${Date.now()}`);
      try {
        const output = run(`init --dir ${baseDir}`);
        expect(output).toContain("schema-flow directory structure");
        expect(existsSync(path.join(baseDir, "schema-flow", "schema"))).toBe(true);
        expect(existsSync(path.join(baseDir, "schema-flow", "pre"))).toBe(true);
        expect(existsSync(path.join(baseDir, "schema-flow", "post"))).toBe(true);
        expect(existsSync(path.join(baseDir, "schema-flow", "mixins"))).toBe(true);
      } finally {
        rmSync(baseDir, { recursive: true, force: true });
      }
    });
  });

  describe("new", () => {
    let project: ReturnType<typeof createTempProject>;

    beforeEach(() => {
      project = createTempProject();
    });

    afterEach(() => {
      project.cleanup();
    });

    it("scaffolds a pre-migration script", () => {
      const output = run(`new pre add_indexes --dir ${project.baseDir}`);
      expect(output).toContain("pre-migration script");

      const files = readdirSync(project.preDir).filter((f) => f.endsWith(".sql"));
      expect(files).toHaveLength(1);
      expect(files[0]).toMatch(/^\d{14}_add_indexes\.sql$/);
    });

    it("scaffolds a post-migration script", () => {
      const output = run(`new post seed_users --dir ${project.baseDir}`);
      expect(output).toContain("post-migration script");

      const files = readdirSync(project.postDir).filter((f) => f.endsWith(".sql"));
      expect(files).toHaveLength(1);
    });

    it("exits with error for invalid subcommand", () => {
      const result = runSafe(`new invalid --dir ${project.baseDir}`);
      expect(result.exitCode).toBe(1);
    });
  });

  describe("database commands", () => {
    const ctx = useTestProject({ closeAppPool: closePool });

    describe("plan", () => {
      it("shows planned operations without applying", () => {
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

        const output = run(`plan --dir ${ctx.project.baseDir}`, {
          DATABASE_URL: ctx.connectionString,
        });

        expect(output).toContain("Dry Run");
        expect(output).toContain("Safe mode");
      });

      it("plan reports blocked destructive operations", async () => {
        // Create a table with an extra column
        await execSql(ctx.connectionString, `CREATE TABLE items (id serial PRIMARY KEY, obsolete text NOT NULL)`);

        writeSchema(
          ctx.project.schemaDir,
          "items.yaml",
          `table: items
columns:
  - name: id
    type: serial
    primary_key: true
`,
        );

        const output = run(`plan --dir ${ctx.project.baseDir}`, {
          DATABASE_URL: ctx.connectionString,
        });

        expect(output).toContain("blocked");
        expect(output).toContain("destructive");
      });
    });

    describe("run", () => {
      it("applies migrations end-to-end", async () => {
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

        run(`run --dir ${ctx.project.baseDir}`, {
          DATABASE_URL: ctx.connectionString,
        });

        expect(await tableExists(ctx.connectionString, "users")).toBe(true);
      });

      it("run migrate only affects schema phase", async () => {
        writeSchema(
          ctx.project.schemaDir,
          "items.yaml",
          `table: items
columns:
  - name: id
    type: serial
    primary_key: true
`,
        );

        run(`run migrate --dir ${ctx.project.baseDir}`, {
          DATABASE_URL: ctx.connectionString,
        });

        expect(await tableExists(ctx.connectionString, "items")).toBe(true);
      });

      it("--allow-destructive drops columns via CLI", async () => {
        await execSql(
          ctx.connectionString,
          `CREATE TABLE things (id serial PRIMARY KEY, name text NOT NULL, old_col text NOT NULL)`,
        );

        writeSchema(
          ctx.project.schemaDir,
          "things.yaml",
          `table: things
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
`,
        );

        run(`run --allow-destructive --dir ${ctx.project.baseDir}`, {
          DATABASE_URL: ctx.connectionString,
        });

        const cols = await getColumns(ctx.connectionString, "things");
        expect(cols).not.toContain("old_col");
        expect(cols).toContain("name");
      });

      it("safe mode preserves columns not in schema file", async () => {
        await execSql(
          ctx.connectionString,
          `CREATE TABLE things (id serial PRIMARY KEY, name text NOT NULL, old_col text NOT NULL)`,
        );

        writeSchema(
          ctx.project.schemaDir,
          "things.yaml",
          `table: things
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
`,
        );

        run(`run --dir ${ctx.project.baseDir}`, {
          DATABASE_URL: ctx.connectionString,
        });

        const cols = await getColumns(ctx.connectionString, "things");
        expect(cols).toContain("old_col"); // Still there!
        expect(cols).toContain("name");
      });
    });
  });

  describe("error handling", () => {
    it("exits 1 when no DATABASE_URL is set", () => {
      const result = runSafe("status --dir /tmp", {
        DATABASE_URL: "",
        SCHEMA_FLOW_DATABASE_URL: "",
      });
      expect(result.exitCode).toBe(1);
    });

    it("exits 1 for unknown command", () => {
      const result = runSafe("nonexistent", {
        DATABASE_URL: "postgresql://localhost:5432/dummy",
      });
      expect(result.exitCode).toBe(1);
    });
  });
});
