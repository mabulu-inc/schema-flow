// test/introspect.test.ts
// Integration tests for PostgreSQL introspection

import { describe, it, expect } from "vitest";
import { useTestClient } from "./helpers.js";
import {
  getExistingTables,
  getTableColumns,
  getTableConstraints,
  getTableTriggers,
  introspectTable,
} from "../src/introspect/index.js";

describe("introspect", () => {
  const ctx = useTestClient();

  it("lists existing tables", async () => {
    await ctx.client.query(`CREATE TABLE users (id serial PRIMARY KEY, name text NOT NULL)`);
    await ctx.client.query(`CREATE TABLE posts (id serial PRIMARY KEY)`);

    const tables = await getExistingTables(ctx.client, "public");
    expect(tables).toContain("users");
    expect(tables).toContain("posts");
  });

  it("returns empty list for empty database", async () => {
    const tables = await getExistingTables(ctx.client, "public");
    expect(tables).toHaveLength(0);
  });

  it("gets column details", async () => {
    await ctx.client.query(`
      CREATE TABLE products (
        id serial PRIMARY KEY,
        name varchar(100) NOT NULL,
        price numeric(10,2),
        description text
      )
    `);

    const columns = await getTableColumns(ctx.client, "products", "public");
    expect(columns).toHaveLength(4);
    expect(columns[0].column_name).toBe("id");
    expect(columns[1].column_name).toBe("name");
    expect(columns[1].character_maximum_length).toBe(100);
  });

  it("gets constraints including primary keys and foreign keys", async () => {
    await ctx.client.query(`CREATE TABLE authors (id serial PRIMARY KEY)`);
    await ctx.client.query(`
      CREATE TABLE books (
        id serial PRIMARY KEY,
        author_id integer REFERENCES authors(id) ON DELETE CASCADE,
        title text UNIQUE NOT NULL
      )
    `);

    const constraints = await getTableConstraints(ctx.client, "books", "public");
    const types = constraints.map((c) => c.constraint_type);
    expect(types).toContain("PRIMARY KEY");
    expect(types).toContain("FOREIGN KEY");
    expect(types).toContain("UNIQUE");

    const fk = constraints.find((c) => c.constraint_type === "FOREIGN KEY");
    expect(fk?.foreign_table_name).toBe("authors");
    expect(fk?.foreign_column_name).toBe("id");
    expect(fk?.delete_rule).toBe("CASCADE");
  });

  it("introspects a full table schema", async () => {
    await ctx.client.query(`
      CREATE TABLE categories (id serial PRIMARY KEY, name varchar(50) UNIQUE NOT NULL)
    `);
    await ctx.client.query(`
      CREATE TABLE items (
        id serial PRIMARY KEY,
        category_id integer REFERENCES categories(id),
        title varchar(200) NOT NULL,
        active boolean NOT NULL DEFAULT true,
        metadata jsonb
      )
    `);

    const schema = await introspectTable(ctx.client, "items", "public");

    expect(schema.table).toBe("items");
    expect(schema.columns).toHaveLength(5);

    const idCol = schema.columns.find((c) => c.name === "id");
    expect(idCol?.type).toBe("serial");
    expect(idCol?.primary_key).toBe(true);

    const catCol = schema.columns.find((c) => c.name === "category_id");
    expect(catCol?.references).toBeDefined();
    expect(catCol?.references?.table).toBe("categories");
    expect(catCol?.references?.column).toBe("id");

    const activeCol = schema.columns.find((c) => c.name === "active");
    expect(activeCol?.type).toBe("boolean");

    const metaCol = schema.columns.find((c) => c.name === "metadata");
    expect(metaCol?.nullable).toBe(true);
  });

  it("introspects composite primary keys", async () => {
    await ctx.client.query(`
      CREATE TABLE tag_map (
        item_id integer NOT NULL,
        tag_id integer NOT NULL,
        PRIMARY KEY (item_id, tag_id)
      )
    `);

    const schema = await introspectTable(ctx.client, "tag_map", "public");
    expect(schema.primary_key).toEqual(["item_id", "tag_id"]);
  });

  it("gets table triggers", async () => {
    await ctx.client.query(`
      CREATE TABLE audit_test (
        id serial PRIMARY KEY,
        updated_at timestamptz NOT NULL DEFAULT now()
      )
    `);
    await ctx.client.query(`
      CREATE OR REPLACE FUNCTION update_timestamp() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN NEW.updated_at = now(); RETURN NEW; END; $$
    `);
    await ctx.client.query(`
      CREATE TRIGGER set_updated_at BEFORE UPDATE ON audit_test
      FOR EACH ROW EXECUTE FUNCTION update_timestamp()
    `);

    const triggers = await getTableTriggers(ctx.client, "audit_test", "public");
    expect(triggers).toHaveLength(1);
    expect(triggers[0].name).toBe("set_updated_at");
    expect(triggers[0].timing).toBe("BEFORE");
    expect(triggers[0].events).toContain("UPDATE");
    expect(triggers[0].function).toBe("update_timestamp");
    expect(triggers[0].for_each).toBe("ROW");
  });

  it("introspectTable includes triggers", async () => {
    await ctx.client.query(`
      CREATE TABLE triggered_table (
        id serial PRIMARY KEY,
        updated_at timestamptz NOT NULL DEFAULT now()
      )
    `);
    await ctx.client.query(`
      CREATE OR REPLACE FUNCTION update_timestamp() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN NEW.updated_at = now(); RETURN NEW; END; $$
    `);
    await ctx.client.query(`
      CREATE TRIGGER set_triggered_table_updated BEFORE UPDATE ON triggered_table
      FOR EACH ROW EXECUTE FUNCTION update_timestamp()
    `);

    const schema = await introspectTable(ctx.client, "triggered_table", "public");
    expect(schema.triggers).toBeDefined();
    expect(schema.triggers).toHaveLength(1);
    expect(schema.triggers![0].name).toBe("set_triggered_table_updated");
  });

  it("returns empty triggers for table without triggers", async () => {
    await ctx.client.query(`CREATE TABLE no_triggers (id serial PRIMARY KEY)`);
    const triggers = await getTableTriggers(ctx.client, "no_triggers", "public");
    expect(triggers).toHaveLength(0);
  });

  it("groups multiple events for same trigger", async () => {
    await ctx.client.query(`CREATE TABLE multi_event (id serial PRIMARY KEY, val text)`);
    await ctx.client.query(`
      CREATE OR REPLACE FUNCTION noop_trigger() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN RETURN NEW; END; $$
    `);
    await ctx.client.query(`
      CREATE TRIGGER multi_trigger BEFORE INSERT OR UPDATE ON multi_event
      FOR EACH ROW EXECUTE FUNCTION noop_trigger()
    `);

    const triggers = await getTableTriggers(ctx.client, "multi_event", "public");
    expect(triggers).toHaveLength(1);
    expect(triggers[0].events).toContain("INSERT");
    expect(triggers[0].events).toContain("UPDATE");
    expect(triggers[0].events).toHaveLength(2);
  });
});
