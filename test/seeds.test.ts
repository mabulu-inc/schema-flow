// test/seeds.test.ts
// Tests for declarative seed data in table YAML

import { describe, it, expect } from "vitest";
import { writeSchema, useTestProject, execSql } from "./helpers.js";
import { resolveConfig } from "../src/core/config.js";
import { runMigrate } from "../src/executor/index.js";
import { closePool } from "../src/core/db.js";
import { logger, LogLevel } from "../src/core/logger.js";

logger.setLevel(LogLevel.SILENT);

describe("seeds", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("inserts seed rows on first run", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "currencies.yaml",
      `table: currencies
columns:
  - name: code
    type: varchar(3)
    primary_key: true
  - name: name
    type: varchar(50)
  - name: symbol
    type: varchar(5)
seeds:
  - {code: USD, name: US Dollar, symbol: $}
  - {code: EUR, name: Euro, symbol: €}
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const res = await execSql(ctx.connectionString, `SELECT code, name, symbol FROM currencies ORDER BY code`);
    expect(res.rows).toEqual([
      { code: "EUR", name: "Euro", symbol: "€" },
      { code: "USD", name: "US Dollar", symbol: "$" },
    ]);
  });

  it("does not burn serial values on re-run", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "statuses.yaml",
      `table: statuses
columns:
  - name: id
    type: serial
    primary_key: true
  - name: label
    type: text
seeds:
  - {id: 1, label: active}
  - {id: 2, label: inactive}
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });

    // Run twice
    await runMigrate(config);
    await closePool();
    await runMigrate(config);

    // Sequence should be set to max(id)=2, so next insert gets id=3
    const res = await execSql(ctx.connectionString, `SELECT last_value FROM statuses_id_seq`);
    expect(Number(res.rows[0].last_value)).toBe(2);
  });

  it("updates changed seed data without inserting duplicates", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "currencies.yaml",
      `table: currencies
columns:
  - name: code
    type: varchar(3)
    primary_key: true
  - name: name
    type: varchar(50)
seeds:
  - {code: USD, name: US Dollar}
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    await runMigrate(config);

    // Change the seed data
    await closePool();
    writeSchema(
      ctx.project.tablesDir,
      "currencies.yaml",
      `table: currencies
columns:
  - name: code
    type: varchar(3)
    primary_key: true
  - name: name
    type: varchar(50)
seeds:
  - {code: USD, name: United States Dollar}
`,
    );

    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const res = await execSql(ctx.connectionString, `SELECT name FROM currencies WHERE code = 'USD'`);
    expect(res.rows[0].name).toBe("United States Dollar");

    const count = await execSql(ctx.connectionString, `SELECT count(*)::int AS cnt FROM currencies`);
    expect(count.rows[0].cnt).toBe(1);
  });

  it("handles nullable columns with null values", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "tags.yaml",
      `table: tags
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
  - name: description
    type: text
    nullable: true
seeds:
  - {id: 1, name: important, description: High priority items}
  - {id: 2, name: archived}
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const res = await execSql(ctx.connectionString, `SELECT id, name, description FROM tags ORDER BY id`);
    expect(res.rows).toEqual([
      { id: 1, name: "important", description: "High priority items" },
      { id: 2, name: "archived", description: null },
    ]);
  });

  it("seeds work with composite primary keys", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "settings.yaml",
      `table: settings
columns:
  - name: scope
    type: text
  - name: key
    type: text
  - name: value
    type: text
primary_key: [scope, key]
seeds:
  - {scope: app, key: theme, value: dark}
  - {scope: app, key: locale, value: en}
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const res = await execSql(ctx.connectionString, `SELECT * FROM settings ORDER BY key`);
    expect(res.rows).toEqual([
      { scope: "app", key: "locale", value: "en" },
      { scope: "app", key: "theme", value: "dark" },
    ]);
  });

  it("second run with unchanged seeds is a no-op", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "currencies.yaml",
      `table: currencies
columns:
  - name: code
    type: varchar(3)
    primary_key: true
  - name: name
    type: varchar(50)
seeds:
  - {code: USD, name: US Dollar}
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    await runMigrate(config);
    await closePool();

    // Second run — seeds should produce zero actual writes
    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const res = await execSql(ctx.connectionString, `SELECT name FROM currencies WHERE code = 'USD'`);
    expect(res.rows[0].name).toBe("US Dollar");
  });

  it("resets serial sequence after seeding explicit IDs", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "statuses.yaml",
      `table: statuses
columns:
  - name: id
    type: serial
    primary_key: true
  - name: label
    type: text
seeds:
  - {id: 1, label: active}
  - {id: 2, label: inactive}
  - {id: 3, label: archived}
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    await runMigrate(config);

    // The sequence should be at 3, so the next insert gets id=4
    const res = await execSql(ctx.connectionString, `INSERT INTO statuses (label) VALUES ('draft') RETURNING id`);
    expect(res.rows[0].id).toBe(4);
  });

  it("falls back to unique column when PK is serial and not in seed data", async () => {
    writeSchema(
      ctx.project.tablesDir,
      "categories.yaml",
      `table: categories
columns:
  - name: id
    type: serial
    primary_key: true
  - name: slug
    type: varchar(100)
    unique: true
  - name: name
    type: varchar(100)
seeds:
  - {slug: general, name: General}
  - {slug: news, name: News}
`,
    );

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
      dryRun: false,
    });
    const result = await runMigrate(config);
    expect(result.success).toBe(true);

    const res = await execSql(ctx.connectionString, `SELECT slug, name FROM categories ORDER BY slug`);
    expect(res.rows).toEqual([
      { slug: "general", name: "General" },
      { slug: "news", name: "News" },
    ]);

    // Re-run is idempotent — no duplicate inserts
    await closePool();
    const result2 = await runMigrate(config);
    expect(result2.success).toBe(true);

    const count = await execSql(ctx.connectionString, `SELECT count(*)::int AS cnt FROM categories`);
    expect(count.rows[0].cnt).toBe(2);
  });
});
