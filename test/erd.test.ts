// test/erd.test.ts
// Tests for Mermaid ERD generation

import { describe, it, expect } from "vitest";
import { generateMermaidErd } from "../src/erd/index.js";
import type { TableSchema } from "../src/schema/types.js";

describe("ERD", () => {
  it("generates basic erDiagram with columns and types", () => {
    const schemas: TableSchema[] = [
      {
        table: "users",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "email", type: "varchar(255)" },
          { name: "name", type: "text" },
        ],
      },
    ];

    const erd = generateMermaidErd(schemas);
    expect(erd).toContain("erDiagram");
    expect(erd).toContain("USERS {");
    expect(erd).toContain("serial id PK");
    expect(erd).toContain("varchar(255) email");
    expect(erd).toContain("text name");
  });

  it("marks FK columns", () => {
    const schemas: TableSchema[] = [
      {
        table: "users",
        columns: [{ name: "id", type: "serial", primary_key: true }],
      },
      {
        table: "orders",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          {
            name: "user_id",
            type: "integer",
            references: { table: "users", column: "id" },
          },
        ],
      },
    ];

    const erd = generateMermaidErd(schemas);
    expect(erd).toContain("integer user_id FK");
  });

  it("marks unique columns with UK", () => {
    const schemas: TableSchema[] = [
      {
        table: "users",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "email", type: "varchar(255)", unique: true },
        ],
      },
    ];

    const erd = generateMermaidErd(schemas);
    expect(erd).toContain("varchar(255) email UK");
  });

  it("generates FK relationships", () => {
    const schemas: TableSchema[] = [
      {
        table: "users",
        columns: [{ name: "id", type: "serial", primary_key: true }],
      },
      {
        table: "orders",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          {
            name: "user_id",
            type: "integer",
            references: { table: "users", column: "id" },
          },
        ],
      },
    ];

    const erd = generateMermaidErd(schemas);
    expect(erd).toContain('USERS ||--o{ ORDERS : "user_id"');
  });

  it("generates one-to-one relationship for unique FK", () => {
    const schemas: TableSchema[] = [
      {
        table: "users",
        columns: [{ name: "id", type: "serial", primary_key: true }],
      },
      {
        table: "profiles",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          {
            name: "user_id",
            type: "integer",
            unique: true,
            references: { table: "users", column: "id" },
          },
        ],
      },
    ];

    const erd = generateMermaidErd(schemas);
    expect(erd).toContain('USERS ||--|| PROFILES : "user_id"');
  });

  it("sanitizes types with commas", () => {
    const schemas: TableSchema[] = [
      {
        table: "products",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "price", type: "numeric(10,2)" },
        ],
      },
    ];

    const erd = generateMermaidErd(schemas);
    expect(erd).toContain("numeric(10_2) price");
    expect(erd).not.toContain("numeric(10,2)");
  });

  it("handles multiple tables with cross-references", () => {
    const schemas: TableSchema[] = [
      {
        table: "tags",
        columns: [{ name: "id", type: "serial", primary_key: true }],
      },
      {
        table: "posts",
        columns: [{ name: "id", type: "serial", primary_key: true }],
      },
      {
        table: "post_tags",
        columns: [
          {
            name: "post_id",
            type: "integer",
            references: { table: "posts", column: "id" },
          },
          {
            name: "tag_id",
            type: "integer",
            references: { table: "tags", column: "id" },
          },
        ],
        primary_key: ["post_id", "tag_id"],
      },
    ];

    const erd = generateMermaidErd(schemas);
    expect(erd).toContain("TAGS {");
    expect(erd).toContain("POSTS {");
    expect(erd).toContain("POST_TAGS {");
    expect(erd).toContain('POSTS ||--o{ POST_TAGS : "post_id"');
    expect(erd).toContain('TAGS ||--o{ POST_TAGS : "tag_id"');
  });

  it("handles empty schema list", () => {
    const erd = generateMermaidErd([]);
    expect(erd).toBe("erDiagram\n");
  });

  it("respects showTypes: false option", () => {
    const schemas: TableSchema[] = [
      {
        table: "users",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "email", type: "varchar(255)" },
        ],
      },
    ];

    const erd = generateMermaidErd(schemas, { showTypes: false });
    expect(erd).toContain("id PK");
    expect(erd).not.toContain("serial");
  });
});
