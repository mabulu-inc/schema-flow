// src/core/config-file.ts
// Load schema-flow.config.yaml with environment variable interpolation

import { readFileSync, existsSync } from "node:fs";
import path from "node:path";
import { parse as parseYaml } from "yaml";

export interface EnvironmentConfig {
  connectionString?: string;
  pgSchema?: string;
  lockTimeout?: string;
  statementTimeout?: string;
}

export interface ConfigFile {
  environments?: Record<string, EnvironmentConfig>;
}

/** Interpolate ${VAR} references in a string using process.env */
function interpolateEnv(value: string): string {
  return value.replace(/\$\{([^}]+)\}/g, (_match, varName) => {
    return process.env[varName] || "";
  });
}

/** Recursively interpolate environment variables in an object */
function interpolateObject(obj: unknown): unknown {
  if (typeof obj === "string") return interpolateEnv(obj);
  if (Array.isArray(obj)) return obj.map(interpolateObject);
  if (obj && typeof obj === "object") {
    const result: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(obj as Record<string, unknown>)) {
      result[key] = interpolateObject(value);
    }
    return result;
  }
  return obj;
}

/** Load a config file from the given directory, or return null if not found */
export function loadConfigFile(baseDir: string): ConfigFile | null {
  const candidates = [
    path.join(baseDir, "schema-flow.config.yaml"),
    path.join(baseDir, "schema-flow.config.yml"),
  ];

  for (const candidate of candidates) {
    if (existsSync(candidate)) {
      const content = readFileSync(candidate, "utf-8");
      const raw = parseYaml(content);
      if (!raw || typeof raw !== "object") return null;
      return interpolateObject(raw) as ConfigFile;
    }
  }

  return null;
}

/** Resolve environment-specific config from a config file */
export function resolveEnvironmentConfig(configFile: ConfigFile, envName: string): EnvironmentConfig | null {
  if (!configFile.environments) return null;
  return configFile.environments[envName] || null;
}
