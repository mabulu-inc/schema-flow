import { describe, it, expect } from "vitest";
import { writeFileSync, mkdirSync, rmSync } from "node:fs";
import path from "node:path";
import { randomBytes } from "node:crypto";
import { loadConfigFile, resolveEnvironmentConfig } from "../src/core/config-file.js";

describe("config file", () => {
  function tempDir(): { dir: string; cleanup: () => void } {
    const dir = path.join("/tmp", `sf_config_test_${randomBytes(4).toString("hex")}`);
    mkdirSync(dir, { recursive: true });
    return { dir, cleanup: () => rmSync(dir, { recursive: true, force: true }) };
  }

  it("loads a config file with environments", () => {
    const { dir, cleanup } = tempDir();
    try {
      writeFileSync(
        path.join(dir, "schema-flow.config.yaml"),
        `
environments:
  development:
    connectionString: postgres://localhost:5432/dev
    pgSchema: public
  production:
    connectionString: postgres://prod:5432/app
    pgSchema: app
`,
        "utf-8",
      );

      const config = loadConfigFile(dir);
      expect(config).not.toBeNull();
      expect(config!.environments).toBeDefined();
      expect(config!.environments!.development.connectionString).toBe("postgres://localhost:5432/dev");
      expect(config!.environments!.production.pgSchema).toBe("app");
    } finally {
      cleanup();
    }
  });

  it("interpolates environment variables", () => {
    const { dir, cleanup } = tempDir();
    try {
      process.env.SF_TEST_DB_URL = "postgres://test:5432/testdb";
      writeFileSync(
        path.join(dir, "schema-flow.config.yaml"),
        `
environments:
  test:
    connectionString: \${SF_TEST_DB_URL}
`,
        "utf-8",
      );

      const config = loadConfigFile(dir);
      expect(config!.environments!.test.connectionString).toBe("postgres://test:5432/testdb");
    } finally {
      delete process.env.SF_TEST_DB_URL;
      cleanup();
    }
  });

  it("returns null when no config file exists", () => {
    const { dir, cleanup } = tempDir();
    try {
      const config = loadConfigFile(dir);
      expect(config).toBeNull();
    } finally {
      cleanup();
    }
  });

  it("resolves environment config", () => {
    const config = {
      environments: {
        dev: { connectionString: "postgres://dev", pgSchema: "public" },
        prod: { connectionString: "postgres://prod", pgSchema: "app" },
      },
    };

    const dev = resolveEnvironmentConfig(config, "dev");
    expect(dev).not.toBeNull();
    expect(dev!.connectionString).toBe("postgres://dev");

    const missing = resolveEnvironmentConfig(config, "staging");
    expect(missing).toBeNull();
  });

  it("loads .yml extension", () => {
    const { dir, cleanup } = tempDir();
    try {
      writeFileSync(
        path.join(dir, "schema-flow.config.yml"),
        `
environments:
  dev:
    connectionString: postgres://dev:5432/db
`,
        "utf-8",
      );

      const config = loadConfigFile(dir);
      expect(config).not.toBeNull();
      expect(config!.environments!.dev.connectionString).toBe("postgres://dev:5432/db");
    } finally {
      cleanup();
    }
  });
});
