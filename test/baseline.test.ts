import { describe, it, expect } from "vitest";
import { useTestProject, writeSchema, tableExists } from "./helpers.js";
import { runBaseline } from "../src/executor/index.js";
import { resolveConfig } from "../src/core/config.js";
import { closePool } from "../src/core/db.js";

describe("baseline", () => {
  const ctx = useTestProject({ closeAppPool: closePool });

  it("records all schema files without executing SQL", async () => {
    writeSchema(ctx.project.schemaDir, "users.yaml", `
table: users
columns:
  - name: id
    type: serial
    primary_key: true
  - name: email
    type: varchar(255)
`);

    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
    });

    const result = await runBaseline(config);
    expect(result.success).toBe(true);
    expect(result.filesRecorded).toBeGreaterThan(0);

    // The table should NOT exist — baseline doesn't create anything
    const exists = await tableExists(ctx.connectionString, "users");
    expect(exists).toBe(false);
  });

  it("returns success with zero files when no schema exists", async () => {
    const config = resolveConfig({
      connectionString: ctx.connectionString,
      baseDir: ctx.project.baseDir,
    });

    const result = await runBaseline(config);
    expect(result.success).toBe(true);
    expect(result.filesRecorded).toBe(0);
  });
});
