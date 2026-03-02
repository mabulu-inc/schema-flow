// src/core/logger.ts
// Structured logger for CI/CD-friendly output

import chalk from "chalk";

export enum LogLevel {
  DEBUG = 0,
  INFO = 1,
  WARN = 2,
  ERROR = 3,
  SILENT = 4,
}

const LEVEL_LABELS: Record<LogLevel, string> = {
  [LogLevel.DEBUG]: "DEBUG",
  [LogLevel.INFO]: "INFO",
  [LogLevel.WARN]: "WARN",
  [LogLevel.ERROR]: "ERROR",
  [LogLevel.SILENT]: "",
};

const LEVEL_COLORS: Record<LogLevel, (s: string) => string> = {
  [LogLevel.DEBUG]: chalk.gray,
  [LogLevel.INFO]: chalk.blue,
  [LogLevel.WARN]: chalk.yellow,
  [LogLevel.ERROR]: chalk.red,
  [LogLevel.SILENT]: (s: string) => s,
};

export class Logger {
  private level: LogLevel;

  constructor(level: LogLevel = LogLevel.INFO) {
    this.level = level;
  }

  setLevel(level: LogLevel): void {
    this.level = level;
  }

  private timestamp(): string {
    return new Date().toISOString();
  }

  private log(level: LogLevel, message: string, meta?: Record<string, unknown>): void {
    if (level < this.level) return;

    const label = LEVEL_COLORS[level](`[${LEVEL_LABELS[level]}]`);
    const ts = chalk.gray(this.timestamp());
    const metaStr = meta ? chalk.gray(` ${JSON.stringify(meta)}`) : "";

    if (level === LogLevel.ERROR) {
      console.error(`${ts} ${label} ${message}${metaStr}`);
    } else {
      console.log(`${ts} ${label} ${message}${metaStr}`);
    }
  }

  debug(message: string, meta?: Record<string, unknown>): void {
    this.log(LogLevel.DEBUG, message, meta);
  }

  info(message: string, meta?: Record<string, unknown>): void {
    this.log(LogLevel.INFO, message, meta);
  }

  warn(message: string, meta?: Record<string, unknown>): void {
    this.log(LogLevel.WARN, message, meta);
  }

  error(message: string, meta?: Record<string, unknown>): void {
    this.log(LogLevel.ERROR, message, meta);
  }

  success(message: string): void {
    if (this.level > LogLevel.INFO) return;
    const ts = chalk.gray(this.timestamp());
    console.log(`${ts} ${chalk.green("[OK]")} ${message}`);
  }

  step(step: string, detail: string): void {
    if (this.level > LogLevel.INFO) return;
    const ts = chalk.gray(this.timestamp());
    console.log(`${ts} ${chalk.cyan(`[${step}]`)} ${detail}`);
  }

  divider(): void {
    if (this.level > LogLevel.INFO) return;
    console.log(chalk.gray("─".repeat(60)));
  }

  banner(text: string): void {
    if (this.level > LogLevel.INFO) return;
    console.log("");
    console.log(chalk.bold.cyan(`  ◆ schema-flow — ${text}`));
    console.log("");
  }
}

export const logger = new Logger();
