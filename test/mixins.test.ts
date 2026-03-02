// test/mixins.test.ts
// Unit tests for mixin loading and expansion

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { createTempProject, writeMixin } from "./helpers.js";
import { loadMixins, expandMixins } from "../src/schema/mixins.js";
import type { TableSchema, MixinSchema } from "../src/schema/types.js";

describe("loadMixins", () => {
  let project: ReturnType<typeof createTempProject>;

  beforeEach(() => {
    project = createTempProject();
  });

  afterEach(() => {
    project.cleanup();
  });

  it("returns empty map when mixins dir is missing", async () => {
    const map = await loadMixins("/nonexistent/path");
    expect(map.size).toBe(0);
  });

  it("loads mixin files from directory", async () => {
    writeMixin(
      project.mixinsDir,
      "timestamps.yaml",
      `mixin: timestamps
columns:
  - name: created_at
    type: timestamptz
    default: now()
`,
    );

    const map = await loadMixins(project.mixinsDir);
    expect(map.size).toBe(1);
    expect(map.has("timestamps")).toBe(true);
    expect(map.get("timestamps")!.columns).toHaveLength(1);
  });

  it("errors on duplicate mixin names", async () => {
    writeMixin(project.mixinsDir, "ts1.yaml", `mixin: timestamps\ncolumns:\n  - name: a\n    type: text\n`);
    writeMixin(project.mixinsDir, "ts2.yaml", `mixin: timestamps\ncolumns:\n  - name: b\n    type: text\n`);

    await expect(loadMixins(project.mixinsDir)).rejects.toThrow("Duplicate mixin name");
  });
});

describe("expandMixins", () => {
  it("passes through tables without use", () => {
    const schemas: TableSchema[] = [
      {
        table: "users",
        columns: [{ name: "id", type: "serial", primary_key: true }],
      },
    ];

    const result = expandMixins(schemas, new Map());
    expect(result).toHaveLength(1);
    expect(result[0].table).toBe("users");
    expect(result[0].use).toBeUndefined();
  });

  it("merges mixin columns before table columns", () => {
    const mixinMap = new Map<string, MixinSchema>([
      [
        "timestamps",
        {
          mixin: "timestamps",
          columns: [
            { name: "created_at", type: "timestamptz", default: "now()" },
            { name: "updated_at", type: "timestamptz", default: "now()" },
          ],
        },
      ],
    ]);

    const schemas: TableSchema[] = [
      {
        table: "users",
        columns: [{ name: "id", type: "serial", primary_key: true }],
        use: ["timestamps"],
      },
    ];

    const result = expandMixins(schemas, mixinMap);
    expect(result[0].columns).toHaveLength(3);
    expect(result[0].columns[0].name).toBe("created_at");
    expect(result[0].columns[1].name).toBe("updated_at");
    expect(result[0].columns[2].name).toBe("id");
    expect(result[0].use).toBeUndefined();
  });

  it("table columns override mixin columns on name clash", () => {
    const mixinMap = new Map<string, MixinSchema>([
      [
        "timestamps",
        {
          mixin: "timestamps",
          columns: [
            { name: "created_at", type: "timestamptz", default: "now()" },
          ],
        },
      ],
    ]);

    const schemas: TableSchema[] = [
      {
        table: "users",
        columns: [
          { name: "id", type: "serial", primary_key: true },
          { name: "created_at", type: "timestamp", default: "now()" }, // overrides mixin
        ],
        use: ["timestamps"],
      },
    ];

    const result = expandMixins(schemas, mixinMap);
    expect(result[0].columns).toHaveLength(2);
    const createdAt = result[0].columns.find((c) => c.name === "created_at");
    expect(createdAt!.type).toBe("timestamp"); // table's version wins
  });

  it("interpolates {table} in trigger, index, and check names", () => {
    const mixinMap = new Map<string, MixinSchema>([
      [
        "timestamps",
        {
          mixin: "timestamps",
          triggers: [
            {
              name: "set_{table}_updated_at",
              timing: "BEFORE",
              events: ["UPDATE"],
              function: "update_timestamp",
              for_each: "ROW",
            },
          ],
          indexes: [{ name: "idx_{table}_created", columns: ["created_at"] }],
          checks: [{ name: "chk_{table}_valid", expression: "1=1" }],
        },
      ],
    ]);

    const schemas: TableSchema[] = [
      {
        table: "orders",
        columns: [{ name: "id", type: "serial", primary_key: true }],
        use: ["timestamps"],
      },
    ];

    const result = expandMixins(schemas, mixinMap);
    expect(result[0].triggers![0].name).toBe("set_orders_updated_at");
    expect(result[0].indexes![0].name).toBe("idx_orders_created");
    expect(result[0].checks![0].name).toBe("chk_orders_valid");
  });

  it("applies multiple mixins in order", () => {
    const mixinMap = new Map<string, MixinSchema>([
      [
        "timestamps",
        {
          mixin: "timestamps",
          columns: [{ name: "created_at", type: "timestamptz" }],
        },
      ],
      [
        "soft_delete",
        {
          mixin: "soft_delete",
          columns: [{ name: "deleted_at", type: "timestamptz", nullable: true }],
        },
      ],
    ]);

    const schemas: TableSchema[] = [
      {
        table: "users",
        columns: [{ name: "id", type: "serial", primary_key: true }],
        use: ["timestamps", "soft_delete"],
      },
    ];

    const result = expandMixins(schemas, mixinMap);
    expect(result[0].columns).toHaveLength(3);
    expect(result[0].columns[0].name).toBe("created_at");
    expect(result[0].columns[1].name).toBe("deleted_at");
    expect(result[0].columns[2].name).toBe("id");
  });

  it("errors on unknown mixin reference", () => {
    const schemas: TableSchema[] = [
      {
        table: "users",
        columns: [{ name: "id", type: "serial", primary_key: true }],
        use: ["nonexistent"],
      },
    ];

    expect(() => expandMixins(schemas, new Map())).toThrow('unknown mixin "nonexistent"');
  });

  it("handles empty mixins gracefully", () => {
    const mixinMap = new Map<string, MixinSchema>([
      ["empty", { mixin: "empty" }],
    ]);

    const schemas: TableSchema[] = [
      {
        table: "users",
        columns: [{ name: "id", type: "serial", primary_key: true }],
        use: ["empty"],
      },
    ];

    const result = expandMixins(schemas, mixinMap);
    expect(result[0].columns).toHaveLength(1);
    expect(result[0].use).toBeUndefined();
  });
});
