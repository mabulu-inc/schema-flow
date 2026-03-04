// test/global-setup.ts
// Ensures the test Postgres container is running before any tests execute.

import { execSync } from "node:child_process";
import path from "node:path";

export default function globalSetup() {
  const script = path.resolve(__dirname, "..", "scripts", "test-db-ensure.sh");
  execSync(`bash "${script}"`, { stdio: "inherit" });
}
