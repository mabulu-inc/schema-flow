import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globalSetup: ["test/global-setup.ts"],
    globals: true,
    testTimeout: 30000,
    hookTimeout: 30000,
    pool: "forks",
    fileParallelism: false,
    reporters: ["verbose"],
    coverage: {
      provider: "v8",
      include: ["src/**/*.ts"],
      exclude: ["src/index.ts"],
      reporter: ["text", "text-summary", "html", "lcov"],
      all: true,
      thresholds: {
        lines: 60,
        functions: 75,
        branches: 50,
        statements: 60,
      },
    },
  },
});
