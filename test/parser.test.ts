// test/parser.test.ts
// Tests for YAML schema parsing and validation

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { writeFileSync } from "node:fs";
import path from "node:path";
import { createTempProject } from "./helpers.js";
import { parseTableFile, parseFunctionFile } from "../src/schema/parser.js";

describe("parseTableFile", () => {
  let project: ReturnType<typeof createTempProject>;

  beforeEach(() => {
    project = createTempProject();
  });

  afterEach(() => {
    project.cleanup();
  });

  it("parses a basic table schema", () => {
    const filePath = path.join(project.schemaDir, "users.yaml");
    writeFileSync(
      filePath,
      `table: users
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: varchar(255)
    unique: true
  - name: bio
    type: text
    nullable: true
`,
      "utf-8",
    );

    const schema = parseTableFile(filePath);

    expect(schema.table).toBe("users");
    expect(schema.columns).toHaveLength(3);
    expect(schema.columns[0]).toMatchObject({ name: "id", type: "serial", primary_key: true });
    expect(schema.columns[1]).toMatchObject({ name: "email", type: "varchar(255)", unique: true });
    expect(schema.columns[2]).toMatchObject({ name: "bio", type: "text", nullable: true });
  });

  it("derives table name from filename when not specified", () => {
    const filePath = path.join(project.schemaDir, "products.yaml");
    writeFileSync(
      filePath,
      `columns:
  - name: id
    type: serial
    primary_key: true
`,
      "utf-8",
    );

    const schema = parseTableFile(filePath);
    expect(schema.table).toBe("products");
  });

  it("parses foreign key references", () => {
    const filePath = path.join(project.schemaDir, "posts.yaml");
    writeFileSync(
      filePath,
      `table: posts
columns:
  - name: id
    type: serial
    primary_key: true
  - name: author_id
    type: integer
    references:
      table: users
      column: id
      on_delete: CASCADE
`,
      "utf-8",
    );

    const schema = parseTableFile(filePath);
    expect(schema.columns[1].references).toMatchObject({
      table: "users",
      column: "id",
      on_delete: "CASCADE",
    });
  });

  it("parses composite primary keys", () => {
    const filePath = path.join(project.schemaDir, "post_tags.yaml");
    writeFileSync(
      filePath,
      `table: post_tags
columns:
  - name: post_id
    type: integer
  - name: tag_id
    type: integer
primary_key: [post_id, tag_id]
`,
      "utf-8",
    );

    const schema = parseTableFile(filePath);
    expect(schema.primary_key).toEqual(["post_id", "tag_id"]);
  });

  it("parses indexes and check constraints", () => {
    const filePath = path.join(project.schemaDir, "orders.yaml");
    writeFileSync(
      filePath,
      `table: orders
columns:
  - name: id
    type: serial
    primary_key: true
  - name: amount
    type: numeric(10,2)
indexes:
  - columns: [amount]
    unique: false
checks:
  - name: chk_positive
    expression: "amount > 0"
`,
      "utf-8",
    );

    const schema = parseTableFile(filePath);
    expect(schema.indexes).toHaveLength(1);
    expect(schema.indexes![0].columns).toEqual(["amount"]);
    expect(schema.checks).toHaveLength(1);
    expect(schema.checks![0].expression).toBe("amount > 0");
  });

  it("throws on missing columns", () => {
    const filePath = path.join(project.schemaDir, "bad.yaml");
    writeFileSync(filePath, "table: bad\n", "utf-8");

    expect(() => parseTableFile(filePath)).toThrow('must define "columns"');
  });

  it("throws on column without name or type", () => {
    const filePath = path.join(project.schemaDir, "bad2.yaml");
    writeFileSync(
      filePath,
      `table: bad2
columns:
  - name: id
`,
      "utf-8",
    );

    expect(() => parseTableFile(filePath)).toThrow('must have "name" and "type"');
  });
});

describe("parseFunctionFile", () => {
  let project: ReturnType<typeof createTempProject>;

  beforeEach(() => {
    project = createTempProject();
  });

  afterEach(() => {
    project.cleanup();
  });

  it("parses a function schema", () => {
    const filePath = path.join(project.schemaDir, "fn_now_utc.yaml");
    writeFileSync(
      filePath,
      `name: now_utc
language: sql
returns: timestamptz
body: SELECT now() AT TIME ZONE 'UTC'
`,
      "utf-8",
    );

    const fn = parseFunctionFile(filePath);
    expect(fn.name).toBe("now_utc");
    expect(fn.language).toBe("sql");
    expect(fn.returns).toBe("timestamptz");
    expect(fn.replace).toBe(true);
  });

  it("throws on missing required fields", () => {
    const filePath = path.join(project.schemaDir, "fn_bad.yaml");
    writeFileSync(filePath, "name: bad_fn\n", "utf-8");

    expect(() => parseFunctionFile(filePath)).toThrow('must define "name" and "body"');
  });
});
