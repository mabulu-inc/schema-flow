// test/parser.test.ts
// Tests for YAML schema parsing and validation

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { writeFileSync } from "node:fs";
import path from "node:path";
import { createTempProject } from "./helpers.js";
import { parseTableFile, parseFunctionFile, parseMixinFile, parsePolicyDef } from "../src/schema/parser.js";

describe("parseTableFile", () => {
  let project: ReturnType<typeof createTempProject>;

  beforeEach(() => {
    project = createTempProject();
  });

  afterEach(() => {
    project.cleanup();
  });

  it("parses a basic table schema", () => {
    const filePath = path.join(project.tablesDir, "users.yaml");
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
    const filePath = path.join(project.tablesDir, "products.yaml");
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
    const filePath = path.join(project.tablesDir, "posts.yaml");
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
    const filePath = path.join(project.tablesDir, "post_tags.yaml");
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
    const filePath = path.join(project.tablesDir, "orders.yaml");
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

  it("parses triggers", () => {
    const filePath = path.join(project.tablesDir, "orders.yaml");
    writeFileSync(
      filePath,
      `table: orders
columns:
  - name: id
    type: serial
    primary_key: true
  - name: updated_at
    type: timestamptz
    default: now()
triggers:
  - name: set_orders_updated_at
    timing: BEFORE
    events: [UPDATE]
    function: update_timestamp
    for_each: ROW
`,
      "utf-8",
    );

    const schema = parseTableFile(filePath);
    expect(schema.triggers).toHaveLength(1);
    expect(schema.triggers![0]).toMatchObject({
      name: "set_orders_updated_at",
      timing: "BEFORE",
      events: ["UPDATE"],
      function: "update_timestamp",
      for_each: "ROW",
    });
  });

  it("parses use array", () => {
    const filePath = path.join(project.tablesDir, "items.yaml");
    writeFileSync(
      filePath,
      `table: items
use: [timestamps, soft_delete]
columns:
  - name: id
    type: serial
    primary_key: true
`,
      "utf-8",
    );

    const schema = parseTableFile(filePath);
    expect(schema.use).toEqual(["timestamps", "soft_delete"]);
  });

  it("throws on missing columns", () => {
    const filePath = path.join(project.tablesDir, "bad.yaml");
    writeFileSync(filePath, "table: bad\n", "utf-8");

    expect(() => parseTableFile(filePath)).toThrow('must define "columns"');
  });

  it("throws on column without name or type", () => {
    const filePath = path.join(project.tablesDir, "bad2.yaml");
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
    const filePath = path.join(project.functionsDir, "now_utc.yaml");
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

  it("accepts function: as alias for name:", () => {
    const filePath = path.join(project.functionsDir, "test.yaml");
    writeFileSync(
      filePath,
      `function: my_func
language: plpgsql
returns: trigger
body: |
  BEGIN
    RETURN NEW;
  END;
`,
      "utf-8",
    );

    const fn = parseFunctionFile(filePath);
    expect(fn.name).toBe("my_func");
    expect(fn.returns).toBe("trigger");
  });

  it("throws on missing required fields", () => {
    const filePath = path.join(project.functionsDir, "bad.yaml");
    writeFileSync(filePath, "name: bad_fn\n", "utf-8");

    expect(() => parseFunctionFile(filePath)).toThrow('must define "name"');
  });
});

describe("parseMixinFile", () => {
  let project: ReturnType<typeof createTempProject>;

  beforeEach(() => {
    project = createTempProject();
  });

  afterEach(() => {
    project.cleanup();
  });

  it("parses a mixin with columns and triggers", () => {
    const filePath = path.join(project.mixinsDir, "timestamps.yaml");
    writeFileSync(
      filePath,
      `mixin: timestamps
columns:
  - name: created_at
    type: timestamptz
    default: now()
  - name: updated_at
    type: timestamptz
    default: now()
triggers:
  - name: set_{table}_updated_at
    timing: BEFORE
    events: [UPDATE]
    function: update_timestamp
    for_each: ROW
`,
      "utf-8",
    );

    const mixin = parseMixinFile(filePath);
    expect(mixin.mixin).toBe("timestamps");
    expect(mixin.columns).toHaveLength(2);
    expect(mixin.triggers).toHaveLength(1);
    expect(mixin.triggers![0].name).toBe("set_{table}_updated_at");
  });

  it("derives mixin name from filename", () => {
    const filePath = path.join(project.mixinsDir, "soft_delete.yaml");
    writeFileSync(
      filePath,
      `columns:
  - name: deleted_at
    type: timestamptz
    nullable: true
`,
      "utf-8",
    );

    const mixin = parseMixinFile(filePath);
    expect(mixin.mixin).toBe("soft_delete");
  });

  it("parses a mixin with indexes", () => {
    const filePath = path.join(project.mixinsDir, "soft_delete.yaml");
    writeFileSync(
      filePath,
      `mixin: soft_delete
columns:
  - name: deleted_at
    type: timestamptz
    nullable: true
indexes:
  - name: idx_{table}_active
    columns: [deleted_at]
    where: "deleted_at IS NULL"
`,
      "utf-8",
    );

    const mixin = parseMixinFile(filePath);
    expect(mixin.indexes).toHaveLength(1);
    expect(mixin.indexes![0].name).toBe("idx_{table}_active");
  });

  it("parses a mixin with rls and policies", () => {
    const filePath = path.join(project.mixinsDir, "tenant_isolation.yaml");
    writeFileSync(
      filePath,
      `mixin: tenant_isolation
rls: true
columns:
  - name: tenant_id
    type: uuid
policies:
  - name: "{table}_tenant_isolation"
    for: ALL
    using: "tenant_id = current_setting('app.tenant_id')::uuid"
    check: "tenant_id = current_setting('app.tenant_id')::uuid"
`,
      "utf-8",
    );

    const mixin = parseMixinFile(filePath);
    expect(mixin.rls).toBe(true);
    expect(mixin.policies).toHaveLength(1);
    expect(mixin.policies![0].name).toBe("{table}_tenant_isolation");
    expect(mixin.policies![0].for).toBe("ALL");
    expect(mixin.policies![0].using).toContain("tenant_id");
  });
});

describe("parsePolicyDef", () => {
  it("validates required fields", () => {
    expect(() => parsePolicyDef({}, "test.yaml")).toThrow('must have "name"');
    expect(() => parsePolicyDef({ name: "p1" }, "test.yaml")).toThrow('must have "for"');
    expect(() => parsePolicyDef({ name: "p1", for: "INVALID" }, "test.yaml")).toThrow("invalid");
  });

  it("parses a valid policy with defaults", () => {
    const policy = parsePolicyDef({ name: "my_policy", for: "SELECT", using: "true" }, "test.yaml");
    expect(policy.name).toBe("my_policy");
    expect(policy.for).toBe("SELECT");
    expect(policy.using).toBe("true");
    expect(policy.permissive).toBe(true);
    expect(policy.to).toBeUndefined();
    expect(policy.check).toBeUndefined();
  });

  it("coerces to as string to array", () => {
    const policy = parsePolicyDef({ name: "p", for: "ALL", to: "app_user" }, "test.yaml");
    expect(policy.to).toEqual(["app_user"]);
  });

  it("accepts to as array", () => {
    const policy = parsePolicyDef({ name: "p", for: "ALL", to: ["role1", "role2"] }, "test.yaml");
    expect(policy.to).toEqual(["role1", "role2"]);
  });

  it("parses permissive: false as RESTRICTIVE", () => {
    const policy = parsePolicyDef({ name: "p", for: "ALL", permissive: false, using: "true" }, "test.yaml");
    expect(policy.permissive).toBe(false);
  });
});

describe("parseTableFile with RLS", () => {
  let project: ReturnType<typeof createTempProject>;

  beforeEach(() => {
    project = createTempProject();
  });

  afterEach(() => {
    project.cleanup();
  });

  it("parses rls, force_rls, and policies", () => {
    const filePath = path.join(project.tablesDir, "orders.yaml");
    writeFileSync(
      filePath,
      `table: orders
rls: true
force_rls: true
columns:
  - name: id
    type: serial
    primary_key: true
  - name: user_id
    type: integer
policies:
  - name: users_see_own_orders
    for: SELECT
    to: app_user
    using: "user_id = get_current_user_id()"
  - name: deny_suspended
    for: ALL
    permissive: false
    using: "NOT is_suspended()"
`,
      "utf-8",
    );

    const schema = parseTableFile(filePath);
    expect(schema.rls).toBe(true);
    expect(schema.force_rls).toBe(true);
    expect(schema.policies).toHaveLength(2);
    expect(schema.policies![0].name).toBe("users_see_own_orders");
    expect(schema.policies![0].for).toBe("SELECT");
    expect(schema.policies![0].to).toEqual(["app_user"]);
    expect(schema.policies![1].permissive).toBe(false);
  });
});

describe("parseFunctionFile with security", () => {
  let project: ReturnType<typeof createTempProject>;

  beforeEach(() => {
    project = createTempProject();
  });

  afterEach(() => {
    project.cleanup();
  });

  it("parses security: definer", () => {
    const filePath = path.join(project.functionsDir, "get_user.yaml");
    writeFileSync(
      filePath,
      `name: get_current_user_id
language: sql
returns: integer
security: definer
body: |
  SELECT id FROM users WHERE auth_id = current_setting('app.auth_id');
`,
      "utf-8",
    );

    const fn = parseFunctionFile(filePath);
    expect(fn.security).toBe("definer");
  });

  it("defaults to no security field when not specified", () => {
    const filePath = path.join(project.functionsDir, "basic.yaml");
    writeFileSync(
      filePath,
      `name: basic_fn
language: sql
returns: void
body: SELECT 1
`,
      "utf-8",
    );

    const fn = parseFunctionFile(filePath);
    expect(fn.security).toBeUndefined();
  });
});
