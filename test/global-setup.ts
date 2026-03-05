// test/global-setup.ts
// Ensures the test Postgres container is running before any tests execute.
// Uses the same infrastructure as the public @mabulu-inc/schema-flow/testing module.

import { ensureTestDb } from "../src/testing/index.js";

export default function globalSetup() {
  ensureTestDb();
}
