// test/drift-comprehensive.test.ts
// Comprehensive drift detection tests for every object type and property variation

import { describe, it, expect } from "vitest";
import { useTestProject, writeSchema, execSql } from "./helpers.js";
import { resolveConfig } from "../src/core/config.js";
import { closePool } from "../src/core/db.js";
import { logger, LogLevel } from "../src/core/logger.js";
import { detectDrift, type DriftItem } from "../src/drift/index.js";

logger.setLevel(LogLevel.SILENT);

/** Helper to run drift detection */
async function drift(ctx: { connectionString: string; project: { baseDir: string } }) {
  const config = resolveConfig({
    connectionString: ctx.connectionString,
    baseDir: ctx.project.baseDir,
  });
  return detectDrift(config);
}

/** Find drift items matching criteria */
function find(items: DriftItem[], opts: Partial<DriftItem>) {
  return items.filter((i) => Object.entries(opts).every(([k, v]) => i[k as keyof DriftItem] === v));
}

describe("drift — columns", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("detects column type mismatch", async () => {
    await execSql(ctx.connectionString, `CREATE TABLE t (id serial PRIMARY KEY, name text)`);
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: varchar(255)
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "column", direction: "mismatch", name: "name" });
    expect(m).toHaveLength(1);
    expect(m[0].details!.some((d) => d.field === "type")).toBe(true);
  });

  it("detects default value mismatch", async () => {
    await execSql(ctx.connectionString, `CREATE TABLE t (id serial PRIMARY KEY, status text DEFAULT 'active')`);
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: status
    type: text
    default: "'pending'"
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "column", direction: "mismatch", name: "status" });
    expect(m).toHaveLength(1);
    expect(m[0].details!.some((d) => d.field === "default")).toBe(true);
  });

  it("detects YAML has default but DB does not", async () => {
    await execSql(ctx.connectionString, `CREATE TABLE t (id serial PRIMARY KEY, status text NOT NULL)`);
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: status
    type: text
    default: "'active'"
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "column", direction: "mismatch", name: "status" });
    expect(m).toHaveLength(1);
    expect(m[0].details!.some((d) => d.field === "default")).toBe(true);
  });

  it("detects unique mismatch — YAML has unique, DB does not", async () => {
    await execSql(ctx.connectionString, `CREATE TABLE t (id serial PRIMARY KEY, email text)`);
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: text
    nullable: true
    unique: true
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "column", direction: "mismatch", name: "email" });
    expect(m).toHaveLength(1);
    expect(m[0].details!.some((d) => d.field === "unique")).toBe(true);
  });

  it("detects unique mismatch — DB has unique, YAML does not", async () => {
    await execSql(ctx.connectionString, `CREATE TABLE t (id serial PRIMARY KEY, email text UNIQUE)`);
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: text
    nullable: true
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "column", direction: "mismatch", name: "email" });
    expect(m).toHaveLength(1);
    expect(m[0].details!.some((d) => d.field === "unique")).toBe(true);
  });

  it("detects FK reference target mismatch", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE orgs (id serial PRIMARY KEY);
       CREATE TABLE users (id serial PRIMARY KEY);
       CREATE TABLE posts (id serial PRIMARY KEY, author_id integer REFERENCES users(id))`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "orgs.yaml",
      `table: orgs
columns:
  - name: id
    type: serial
    primary_key: true
`,
    );
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
    writeSchema(
      ctx.project.tablesDir,
      "posts.yaml",
      `table: posts
columns:
  - name: id
    type: serial
    primary_key: true
  - name: author_id
    type: integer
    nullable: true
    references:
      table: orgs
      column: id
`,
    );
    const report = await drift(ctx);
    // Should detect that the FK target differs (users vs orgs)
    const m = find(report.items, { category: "column", direction: "mismatch", name: "author_id" });
    expect(m.length).toBeGreaterThanOrEqual(1);
    expect(m[0].details!.some((d) => d.field === "fk_table" || d.field === "fk_column")).toBe(true);
  });

  it("detects FK on_delete mismatch", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE parents (id serial PRIMARY KEY);
       CREATE TABLE children (id serial PRIMARY KEY, parent_id integer REFERENCES parents(id) ON DELETE CASCADE)`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "parents.yaml",
      `table: parents
columns:
  - name: id
    type: serial
    primary_key: true
`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "children.yaml",
      `table: children
columns:
  - name: id
    type: serial
    primary_key: true
  - name: parent_id
    type: integer
    nullable: true
    references:
      table: parents
      column: id
      on_delete: SET NULL
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "column", direction: "mismatch", name: "parent_id" });
    expect(m.length).toBeGreaterThanOrEqual(1);
    expect(m[0].details!.some((d) => d.field === "on_delete")).toBe(true);
  });
});

describe("drift — indexes", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("detects missing index", async () => {
    await execSql(ctx.connectionString, `CREATE TABLE t (id serial PRIMARY KEY, email text)`);
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: text
    nullable: true
indexes:
  - name: idx_t_email
    columns: [email]
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "index", direction: "missing_from_db", name: "idx_t_email" });
    expect(m).toHaveLength(1);
  });

  it("detects extra index in DB", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY, email text);
       CREATE INDEX idx_t_email ON t (email)`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: text
    nullable: true
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "index", direction: "extra_in_db", name: "idx_t_email" });
    expect(m).toHaveLength(1);
  });

  it("detects index column mismatch", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY, a text, b text);
       CREATE INDEX idx_t_ab ON t (a)`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: a
    type: text
    nullable: true
  - name: b
    type: text
    nullable: true
indexes:
  - name: idx_t_ab
    columns: [a, b]
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "index", direction: "mismatch", name: "idx_t_ab" });
    expect(m).toHaveLength(1);
    expect(m[0].description).toContain("columns");
  });

  it("detects index unique mismatch", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY, email text);
       CREATE INDEX idx_t_email ON t (email)`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: text
    nullable: true
indexes:
  - name: idx_t_email
    columns: [email]
    unique: true
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "index", direction: "mismatch", name: "idx_t_email" });
    expect(m).toHaveLength(1);
    expect(m[0].description).toContain("unique");
  });

  it("detects index method mismatch", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY, data jsonb);
       CREATE INDEX idx_t_data ON t USING gin (data)`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: data
    type: jsonb
    nullable: true
indexes:
  - name: idx_t_data
    columns: [data]
    method: btree
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "index", direction: "mismatch", name: "idx_t_data" });
    expect(m).toHaveLength(1);
    expect(m[0].description).toContain("method");
  });

  it("detects index WHERE clause mismatch", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY, active boolean DEFAULT true);
       CREATE INDEX idx_t_active ON t (id) WHERE (active = true)`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: active
    type: boolean
    default: "true"
indexes:
  - name: idx_t_active
    columns: [id]
    where: "(active = false)"
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "index", direction: "mismatch", name: "idx_t_active" });
    expect(m).toHaveLength(1);
    expect(m[0].description).toContain("where");
  });

  it("no drift when index matches fully", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY, email text);
       CREATE INDEX idx_t_email ON t (email)`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: text
    nullable: true
indexes:
  - name: idx_t_email
    columns: [email]
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "index" });
    expect(m).toHaveLength(0);
  });
});

describe("drift — unique constraints", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("detects missing unique constraint", async () => {
    await execSql(ctx.connectionString, `CREATE TABLE t (id serial PRIMARY KEY, a text, b text)`);
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: a
    type: text
    nullable: true
  - name: b
    type: text
    nullable: true
unique_constraints:
  - name: uq_t_a_b
    columns: [a, b]
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "constraint", direction: "missing_from_db", name: "uq_t_a_b" });
    expect(m).toHaveLength(1);
  });

  it("detects extra unique constraint", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY, a text, b text, CONSTRAINT uq_t_a_b UNIQUE (a, b))`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: a
    type: text
    nullable: true
  - name: b
    type: text
    nullable: true
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "constraint", direction: "extra_in_db", name: "uq_t_a_b" });
    expect(m).toHaveLength(1);
  });

  it("detects unique constraint column mismatch", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY, a text, b text, c text, CONSTRAINT uq_t UNIQUE (a, b))`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: a
    type: text
    nullable: true
  - name: b
    type: text
    nullable: true
  - name: c
    type: text
    nullable: true
unique_constraints:
  - name: uq_t
    columns: [a, c]
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "constraint", direction: "mismatch", name: "uq_t" });
    expect(m).toHaveLength(1);
    expect(m[0].details!.some((d) => d.field === "columns")).toBe(true);
  });
});

describe("drift — triggers", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("detects missing trigger", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY, updated_at timestamptz);
       CREATE FUNCTION noop() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: updated_at
    type: timestamptz
    nullable: true
triggers:
  - name: trg_t_updated
    timing: BEFORE
    events: [UPDATE]
    for_each: ROW
    function: noop
`,
    );
    writeSchema(
      ctx.project.functionsDir,
      "noop.yaml",
      `name: noop
returns: trigger
language: plpgsql
body: |
  BEGIN RETURN NEW; END;
replace: true
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "trigger", direction: "missing_from_db", name: "trg_t_updated" });
    expect(m).toHaveLength(1);
  });

  it("detects extra trigger", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY);
       CREATE FUNCTION noop() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
       CREATE TRIGGER trg_extra BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION noop()`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
`,
    );
    writeSchema(
      ctx.project.functionsDir,
      "noop.yaml",
      `name: noop
returns: trigger
language: plpgsql
body: |
  BEGIN RETURN NEW; END;
replace: true
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "trigger", direction: "extra_in_db", name: "trg_extra" });
    expect(m).toHaveLength(1);
  });

  it("detects trigger property mismatch (timing)", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY);
       CREATE FUNCTION noop() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
       CREATE TRIGGER trg_t BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION noop()`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
triggers:
  - name: trg_t
    timing: AFTER
    events: [INSERT]
    for_each: ROW
    function: noop
`,
    );
    writeSchema(
      ctx.project.functionsDir,
      "noop.yaml",
      `name: noop
returns: trigger
language: plpgsql
body: |
  BEGIN RETURN NEW; END;
replace: true
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "trigger", direction: "mismatch", name: "trg_t" });
    expect(m).toHaveLength(1);
    expect(m[0].details!.some((d) => d.field === "timing")).toBe(true);
  });

  it("detects trigger event mismatch", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY);
       CREATE FUNCTION noop() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
       CREATE TRIGGER trg_t BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION noop()`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
triggers:
  - name: trg_t
    timing: BEFORE
    events: [INSERT, UPDATE]
    for_each: ROW
    function: noop
`,
    );
    writeSchema(
      ctx.project.functionsDir,
      "noop.yaml",
      `name: noop
returns: trigger
language: plpgsql
body: |
  BEGIN RETURN NEW; END;
replace: true
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "trigger", direction: "mismatch", name: "trg_t" });
    expect(m).toHaveLength(1);
    expect(m[0].details!.some((d) => d.field === "events")).toBe(true);
  });

  it("detects trigger function mismatch", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY);
       CREATE FUNCTION noop() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
       CREATE FUNCTION other_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
       CREATE TRIGGER trg_t BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION noop()`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
triggers:
  - name: trg_t
    timing: BEFORE
    events: [INSERT]
    for_each: ROW
    function: other_fn
`,
    );
    writeSchema(
      ctx.project.functionsDir,
      "noop.yaml",
      `name: noop
returns: trigger
language: plpgsql
body: |
  BEGIN RETURN NEW; END;
replace: true
`,
    );
    writeSchema(
      ctx.project.functionsDir,
      "other.yaml",
      `name: other_fn
returns: trigger
language: plpgsql
body: |
  BEGIN RETURN NEW; END;
replace: true
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "trigger", direction: "mismatch", name: "trg_t" });
    expect(m).toHaveLength(1);
    expect(m[0].details!.some((d) => d.field === "function")).toBe(true);
  });
});

describe("drift — policies", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("detects missing policy", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY);
       ALTER TABLE t ENABLE ROW LEVEL SECURITY`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
rls: true
policies:
  - name: allow_all
    for: ALL
    to: [PUBLIC]
    using: "true"
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "policy", direction: "missing_from_db", name: "allow_all" });
    expect(m).toHaveLength(1);
  });

  it("detects extra policy", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY);
       ALTER TABLE t ENABLE ROW LEVEL SECURITY;
       CREATE POLICY allow_all ON t FOR ALL TO PUBLIC USING (true)`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
rls: true
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "policy", direction: "extra_in_db", name: "allow_all" });
    expect(m).toHaveLength(1);
  });

  it("detects policy USING expression mismatch", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY, owner_id integer);
       ALTER TABLE t ENABLE ROW LEVEL SECURITY;
       CREATE POLICY row_owner ON t FOR ALL TO PUBLIC USING (owner_id = 1)`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: owner_id
    type: integer
    nullable: true
rls: true
policies:
  - name: row_owner
    for: ALL
    to: [PUBLIC]
    using: "(owner_id = 2)"
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "policy", direction: "mismatch", name: "row_owner" });
    expect(m).toHaveLength(1);
    expect(m[0].details!.some((d) => d.field === "using")).toBe(true);
  });

  it("detects policy command mismatch", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY);
       ALTER TABLE t ENABLE ROW LEVEL SECURITY;
       CREATE POLICY pol ON t FOR SELECT TO PUBLIC USING (true)`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
rls: true
policies:
  - name: pol
    for: ALL
    to: [PUBLIC]
    using: "true"
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "policy", direction: "mismatch", name: "pol" });
    expect(m).toHaveLength(1);
    expect(m[0].details!.some((d) => d.field === "for")).toBe(true);
  });
});

describe("drift — RLS", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("detects RLS enabled in YAML but not in DB", async () => {
    await execSql(ctx.connectionString, `CREATE TABLE t (id serial PRIMARY KEY)`);
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
rls: true
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "rls", direction: "mismatch" });
    expect(m).toHaveLength(1);
  });

  it("detects RLS enabled in DB but not in YAML", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY);
       ALTER TABLE t ENABLE ROW LEVEL SECURITY`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "rls", direction: "mismatch" });
    expect(m).toHaveLength(1);
  });

  it("detects force_rls mismatch", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY);
       ALTER TABLE t ENABLE ROW LEVEL SECURITY;
       ALTER TABLE t FORCE ROW LEVEL SECURITY`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
rls: true
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "rls", direction: "mismatch" });
    expect(m.length).toBeGreaterThanOrEqual(1);
    expect(m.some((i) => i.details?.some((d) => d.field === "force_rls"))).toBe(true);
  });
});

describe("drift — check constraints", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("detects missing check constraint", async () => {
    await execSql(ctx.connectionString, `CREATE TABLE t (id serial PRIMARY KEY, price integer)`);
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: price
    type: integer
    nullable: true
checks:
  - name: chk_positive_price
    expression: "price > 0"
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { direction: "missing_from_db", name: "chk_positive_price" });
    expect(m).toHaveLength(1);
  });

  it("detects extra check constraint", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY, price integer, CONSTRAINT chk_positive CHECK (price > 0))`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: price
    type: integer
    nullable: true
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { direction: "extra_in_db", name: "chk_positive" });
    expect(m).toHaveLength(1);
  });

  it("detects check expression mismatch", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY, price integer, CONSTRAINT chk_price CHECK (price > 0))`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: price
    type: integer
    nullable: true
checks:
  - name: chk_price
    expression: "price > 10"
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { direction: "mismatch", name: "chk_price" });
    expect(m).toHaveLength(1);
    expect(m[0].details!.some((d) => d.field === "expression")).toBe(true);
  });
});

describe("drift — enums", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("detects missing enum", async () => {
    writeSchema(
      ctx.project.enumsDir,
      "status.yaml",
      `enum: status_type
values: [active, inactive]
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "enum", direction: "missing_from_db", name: "status_type" });
    expect(m).toHaveLength(1);
  });

  it("detects extra enum value in DB", async () => {
    await execSql(ctx.connectionString, `CREATE TYPE status_type AS ENUM ('active', 'inactive', 'archived')`);
    writeSchema(
      ctx.project.enumsDir,
      "status.yaml",
      `enum: status_type
values: [active, inactive]
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "enum", direction: "mismatch", name: "status_type" });
    expect(m).toHaveLength(1);
    expect(m[0].details!.some((d) => d.field === "extra_values" && d.actual.includes("archived"))).toBe(true);
  });

  it("detects missing enum value in DB", async () => {
    await execSql(ctx.connectionString, `CREATE TYPE status_type AS ENUM ('active')`);
    writeSchema(
      ctx.project.enumsDir,
      "status.yaml",
      `enum: status_type
values: [active, inactive]
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "enum", direction: "mismatch", name: "status_type" });
    expect(m).toHaveLength(1);
    expect(m[0].details!.some((d) => d.field === "missing_values" && d.expected.includes("inactive"))).toBe(true);
  });

  it("no drift when enum values match", async () => {
    await execSql(ctx.connectionString, `CREATE TYPE status_type AS ENUM ('active', 'inactive')`);
    writeSchema(
      ctx.project.enumsDir,
      "status.yaml",
      `enum: status_type
values: [active, inactive]
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "enum" });
    expect(m).toHaveLength(0);
  });
});

describe("drift — views", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("detects missing view", async () => {
    await execSql(ctx.connectionString, `CREATE TABLE t (id serial PRIMARY KEY, name text)`);
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
    nullable: true
`,
    );
    writeSchema(
      ctx.project.viewsDir,
      "t.yaml",
      `view: v_t
query: "SELECT id, name FROM t"
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "view", direction: "missing_from_db", name: "v_t" });
    expect(m).toHaveLength(1);
  });

  it("detects extra view in DB", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY, name text);
       CREATE VIEW v_extra AS SELECT id FROM t`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
    nullable: true
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "view", direction: "extra_in_db", name: "v_extra" });
    expect(m).toHaveLength(1);
  });

  it("detects view query mismatch", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY, name text);
       CREATE VIEW v_t AS SELECT id FROM t`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
    nullable: true
`,
    );
    writeSchema(
      ctx.project.viewsDir,
      "t.yaml",
      `view: v_t
query: "SELECT id, name FROM t"
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "view", direction: "mismatch", name: "v_t" });
    expect(m).toHaveLength(1);
  });
});

describe("drift — materialized views", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("detects missing materialized view", async () => {
    await execSql(ctx.connectionString, `CREATE TABLE t (id serial PRIMARY KEY, name text)`);
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
    nullable: true
`,
    );
    writeSchema(
      ctx.project.viewsDir,
      "mv_t.yaml",
      `materialized_view: mv_t
query: "SELECT id, name FROM t"
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, {
      category: "materialized_view",
      direction: "missing_from_db",
      name: "mv_t",
    });
    expect(m).toHaveLength(1);
  });

  it("detects materialized view query mismatch", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY, name text);
       CREATE MATERIALIZED VIEW mv_t AS SELECT id FROM t`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
    nullable: true
`,
    );
    writeSchema(
      ctx.project.viewsDir,
      "mv_t.yaml",
      `materialized_view: mv_t
query: "SELECT id, name FROM t"
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "materialized_view", direction: "mismatch", name: "mv_t" });
    expect(m).toHaveLength(1);
  });
});

describe("drift — functions", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("detects function language mismatch", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE FUNCTION add_one(x integer) RETURNS integer LANGUAGE sql AS $$ SELECT x + 1; $$`,
    );
    writeSchema(
      ctx.project.functionsDir,
      "add_one.yaml",
      `name: add_one
returns: integer
language: plpgsql
args: "x integer"
body: |
  SELECT x + 1;
replace: true
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "function", direction: "mismatch", name: "add_one" });
    expect(m).toHaveLength(1);
    expect(m[0].details!.some((d) => d.field === "language")).toBe(true);
  });

  it("detects function security mismatch", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE FUNCTION my_fn() RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $$ BEGIN END; $$`,
    );
    writeSchema(
      ctx.project.functionsDir,
      "my_fn.yaml",
      `name: my_fn
returns: void
language: plpgsql
args: ""
body: |
  BEGIN END;
replace: true
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "function", direction: "mismatch", name: "my_fn" });
    expect(m).toHaveLength(1);
    expect(m[0].details!.some((d) => d.field === "security")).toBe(true);
  });

  it("detects missing function", async () => {
    writeSchema(
      ctx.project.functionsDir,
      "missing.yaml",
      `name: missing_fn
returns: void
language: plpgsql
args: ""
body: |
  BEGIN END;
replace: true
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "function", direction: "missing_from_db", name: "missing_fn" });
    expect(m).toHaveLength(1);
  });
});

describe("drift — extensions", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("detects missing extension", async () => {
    writeSchema(ctx.project.sfDir, "extensions.yaml", `extensions:\n  - pgcrypto\n`);
    // pgcrypto is not installed by default
    const report = await drift(ctx);
    const m = find(report.items, { category: "extension", direction: "missing_from_db", name: "pgcrypto" });
    expect(m).toHaveLength(1);
  });
});

describe("drift — generated columns", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("detects generated column expression mismatch", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY, a integer, b integer, total integer GENERATED ALWAYS AS (a + b) STORED)`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: a
    type: integer
    nullable: true
  - name: b
    type: integer
    nullable: true
  - name: total
    type: integer
    nullable: true
    generated: "(a * b)"
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "column", direction: "mismatch", name: "total" });
    expect(m).toHaveLength(1);
    expect(m[0].details!.some((d) => d.field === "generated")).toBe(true);
  });

  it("detects YAML has generated but DB does not", async () => {
    await execSql(ctx.connectionString, `CREATE TABLE t (id serial PRIMARY KEY, a integer, b integer, total integer)`);
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: a
    type: integer
    nullable: true
  - name: b
    type: integer
    nullable: true
  - name: total
    type: integer
    nullable: true
    generated: "(a + b)"
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "column", direction: "mismatch", name: "total" });
    expect(m).toHaveLength(1);
    expect(m[0].details!.some((d) => d.field === "generated")).toBe(true);
  });

  it("detects DB has generated but YAML does not", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY, a integer, b integer, total integer GENERATED ALWAYS AS (a + b) STORED)`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: a
    type: integer
    nullable: true
  - name: b
    type: integer
    nullable: true
  - name: total
    type: integer
    nullable: true
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "column", direction: "mismatch", name: "total" });
    expect(m).toHaveLength(1);
    expect(m[0].details!.some((d) => d.field === "generated")).toBe(true);
  });
});

describe("drift — trigger WHEN clause", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("detects trigger WHEN condition mismatch", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY, status text);
       CREATE FUNCTION noop() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
       CREATE TRIGGER trg_t BEFORE UPDATE ON t FOR EACH ROW WHEN (OLD.status IS DISTINCT FROM NEW.status) EXECUTE FUNCTION noop()`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: status
    type: text
    nullable: true
triggers:
  - name: trg_t
    timing: BEFORE
    events: [UPDATE]
    for_each: ROW
    when: "(OLD.status <> NEW.status)"
    function: noop
`,
    );
    writeSchema(
      ctx.project.functionsDir,
      "noop.yaml",
      `name: noop
returns: trigger
language: plpgsql
body: |
  BEGIN RETURN NEW; END;
replace: true
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "trigger", direction: "mismatch", name: "trg_t" });
    expect(m).toHaveLength(1);
    expect(m[0].details!.some((d) => d.field === "when")).toBe(true);
  });

  it("detects YAML has WHEN but DB does not", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY, status text);
       CREATE FUNCTION noop() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
       CREATE TRIGGER trg_t BEFORE UPDATE ON t FOR EACH ROW EXECUTE FUNCTION noop()`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: status
    type: text
    nullable: true
triggers:
  - name: trg_t
    timing: BEFORE
    events: [UPDATE]
    for_each: ROW
    when: "(OLD.status IS DISTINCT FROM NEW.status)"
    function: noop
`,
    );
    writeSchema(
      ctx.project.functionsDir,
      "noop.yaml",
      `name: noop
returns: trigger
language: plpgsql
body: |
  BEGIN RETURN NEW; END;
replace: true
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "trigger", direction: "mismatch", name: "trg_t" });
    expect(m).toHaveLength(1);
    expect(m[0].details!.some((d) => d.field === "when")).toBe(true);
  });
});

describe("drift — policy TO roles", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("detects policy role mismatch", async () => {
    await execSql(
      ctx.connectionString,
      `DO $$ BEGIN
         IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'test_role_a') THEN CREATE ROLE test_role_a; END IF;
         IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'test_role_b') THEN CREATE ROLE test_role_b; END IF;
       END $$;
       CREATE TABLE t (id serial PRIMARY KEY);
       ALTER TABLE t ENABLE ROW LEVEL SECURITY;
       CREATE POLICY pol ON t FOR ALL TO test_role_a USING (true)`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
rls: true
policies:
  - name: pol
    for: ALL
    to: [test_role_b]
    using: "true"
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "policy", direction: "mismatch", name: "pol" });
    expect(m).toHaveLength(1);
    expect(m[0].details!.some((d) => d.field === "to")).toBe(true);
  });
});

describe("drift — policy permissive", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("detects permissive vs restrictive mismatch", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY);
       ALTER TABLE t ENABLE ROW LEVEL SECURITY;
       CREATE POLICY pol ON t AS RESTRICTIVE FOR ALL TO PUBLIC USING (true)`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
rls: true
policies:
  - name: pol
    for: ALL
    to: [PUBLIC]
    using: "true"
    permissive: true
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "policy", direction: "mismatch", name: "pol" });
    expect(m).toHaveLength(1);
    expect(m[0].details!.some((d) => d.field === "permissive")).toBe(true);
  });
});

describe("drift — function args", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("detects function argument mismatch", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE FUNCTION test_fn(x integer) RETURNS integer LANGUAGE sql AS $$ SELECT x + 1; $$`,
    );
    writeSchema(
      ctx.project.functionsDir,
      "test_fn.yaml",
      `name: test_fn
returns: integer
language: sql
args: "x integer, y integer"
body: |
  SELECT x + y;
replace: true
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "function", direction: "mismatch", name: "test_fn" });
    expect(m).toHaveLength(1);
    expect(m[0].details!.some((d) => d.field === "args")).toBe(true);
  });
});

describe("drift — index INCLUDE", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("detects index INCLUDE columns mismatch", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY, email text, name text, age integer);
       CREATE INDEX idx_t_email ON t (email) INCLUDE (name)`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: text
    nullable: true
  - name: name
    type: text
    nullable: true
  - name: age
    type: integer
    nullable: true
indexes:
  - name: idx_t_email
    columns: [email]
    include: [age]
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "index", direction: "mismatch", name: "idx_t_email" });
    expect(m).toHaveLength(1);
    expect(m[0].description).toContain("include");
  });

  it("detects YAML has INCLUDE but DB does not", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY, email text, name text);
       CREATE INDEX idx_t_email ON t (email)`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: text
    nullable: true
  - name: name
    type: text
    nullable: true
indexes:
  - name: idx_t_email
    columns: [email]
    include: [name]
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "index", direction: "mismatch", name: "idx_t_email" });
    expect(m).toHaveLength(1);
    expect(m[0].description).toContain("include");
  });
});

describe("drift — materialized view indexes", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("detects missing MV index", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY, name text);
       CREATE MATERIALIZED VIEW mv_t AS SELECT id, name FROM t`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
    nullable: true
`,
    );
    writeSchema(
      ctx.project.viewsDir,
      "mv_t.yaml",
      `materialized_view: mv_t
query: "SELECT id, name FROM t"
indexes:
  - name: idx_mv_t_id
    columns: [id]
    unique: true
`,
    );
    const report = await drift(ctx);
    // Should detect the missing index on MV
    const m = find(report.items, { category: "index", direction: "missing_from_db", name: "idx_mv_t_id" });
    expect(m).toHaveLength(1);
  });

  it("detects extra MV index in DB", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY, name text);
       CREATE MATERIALIZED VIEW mv_t AS SELECT id, name FROM t;
       CREATE UNIQUE INDEX idx_mv_t_id ON mv_t (id)`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: name
    type: text
    nullable: true
`,
    );
    writeSchema(
      ctx.project.viewsDir,
      "mv_t.yaml",
      `materialized_view: mv_t
query: "SELECT id, name FROM t"
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "index", direction: "extra_in_db", name: "idx_mv_t_id" });
    expect(m).toHaveLength(1);
  });
});

describe("drift — index opclass", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("detects index opclass mismatch", async () => {
    await execSql(
      ctx.connectionString,
      `CREATE TABLE t (id serial PRIMARY KEY, data jsonb);
       CREATE INDEX idx_t_data ON t USING gin (data jsonb_path_ops)`,
    );
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
  - name: data
    type: jsonb
    nullable: true
indexes:
  - name: idx_t_data
    columns: [data]
    method: gin
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "index", direction: "mismatch", name: "idx_t_data" });
    expect(m).toHaveLength(1);
    expect(m[0].description).toContain("opclass");
  });
});

describe("drift — PK constraint name", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("detects PK constraint name mismatch", async () => {
    await execSql(ctx.connectionString, `CREATE TABLE t (id serial, CONSTRAINT pk_t PRIMARY KEY (id))`);
    writeSchema(
      ctx.project.tablesDir,
      "t.yaml",
      `table: t
columns:
  - name: id
    type: serial
    primary_key: true
primary_key_name: pk_t_custom
`,
    );
    const report = await drift(ctx);
    const m = find(report.items, { category: "constraint", direction: "mismatch", name: "primary_key" });
    expect(m).toHaveLength(1);
    expect(m[0].details!.some((d) => d.field === "primary_key_name")).toBe(true);
  });
});
