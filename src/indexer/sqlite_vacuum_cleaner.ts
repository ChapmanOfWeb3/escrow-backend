import type Database from "better-sqlite3";
import logger from "../utils/logger.js";

// ---------------------------------------------------------------------------
// SQLite vacuum cleaner (#193)
// ---------------------------------------------------------------------------
//
// Extended features:
//   - Dynamic polling frequency (Issue 1): adjustVacuumPollingInterval()
//     increases wait delays when the database is idle (no rows pruned),
//     backing off up to MAX_VACUUM_POLL_INTERVAL_MS.
//
//   - Dynamic ledger range imports (Issue 3): pruneEventsInLedgerRange()
//     accepts custom start/end ledger values so callers can import and prune
//     arbitrary historical windows.
//
//   - Schema migration check utilities (Issue 4): validateVacuumSchema()
//     verifies the required tables/columns exist before the cleaner starts,
//     failing fast when the database state is out of sync.
//
//   - Exponential backoff retry (#343): withVacuumRetrySync() and
//     withVacuumRetry() retry RPC connection timeouts with exponentially
//     increasing delays up to a configurable ceiling, so transient connection
//     dropouts don't abort the vacuum cycle.
//
// This module prunes stale rows from the `events` table and reclaims the
// disk space they occupied.
//
// IMPORTANT — transaction isolation vs. VACUUM:
// SQLite does NOT allow the `VACUUM` command to run inside an explicit
// transaction (`db.exec("VACUUM")` while a transaction is active throws
// `SqliteError: cannot VACUUM from within a transaction`). Because of that
// hard engine constraint, "transaction isolation" for this module cannot
// mean wrapping VACUUM itself in a transaction. Instead:
//
//   1. The data-pruning/cleanup step that decides what to delete
//      (pruneOldEvents) runs atomically inside a `db.transaction(...)`
//      block, mirroring the pattern used by `insertEventBatch()` in
//      `src/indexer/db.ts` — if anything fails partway through, the whole
//      deletion rolls back and no rows are left half-deleted.
//   2. The separate, non-transactional `VACUUM` command (runVacuum) only
//      runs afterward, once pruning has fully committed — never nested
//      inside a transaction.
//
// `runVacuumCleanup()` orchestrates these two steps in the correct order.

/**
 * Default retention window (in days) used when the caller doesn't specify
 * one. Events older than this are eligible for pruning.
 */
export const DEFAULT_RETENTION_DAYS = 90;

export interface VacuumCleanupOptions {
  /** How many days of events to retain. Defaults to DEFAULT_RETENTION_DAYS. */
  retentionDays?: number;
}

export interface VacuumCleanupResult {
  /** Number of event rows deleted during the pruning step. */
  prunedEvents: number;
  /** Whether the VACUUM step ran successfully. */
  vacuumed: boolean;
}

export const ERROR_CODES = {
  INVALID_RETENTION: "VACUUM_INVALID_RETENTION",
} as const;

export type ErrorCode = (typeof ERROR_CODES)[keyof typeof ERROR_CODES];

/**
 * Validates a retentionDays value. Must be a finite, positive integer.
 * Zero, negative, non-integer, NaN, and Infinity values are all rejected —
 * a zero or negative retention window has no sane "keep nothing older than
 * X days" interpretation and would risk pruning everything (or throwing on
 * a nonsensical SQL interval).
 */
export function validateRetentionDays(
  retentionDays: number
): { ok: true } | { ok: false; error: string; code: "VACUUM_INVALID_RETENTION" } {
  if (
    typeof retentionDays !== "number" ||
    !Number.isFinite(retentionDays) ||
    !Number.isInteger(retentionDays) ||
    retentionDays <= 0
  ) {
    return {
      ok: false,
      error: `retentionDays must be a positive integer, got: ${retentionDays}`,
      code: ERROR_CODES.INVALID_RETENTION,
    };
  }
  return { ok: true };
}

/**
 * Deletes events older than `retentionDays`, computing the cutoff timestamp
 * against the database's own clock (via SQLite's `datetime('now', ...)`
 * rather than in JS) so the comparison is always consistent with the
 * `created_at DATETIME DEFAULT CURRENT_TIMESTAMP` column it's compared
 * against.
 *
 * The deletion is wrapped in `db.transaction(...)` — this is the
 * transactional core of the vacuum cleaner. Even though a single DELETE
 * statement is atomic on its own, wrapping it explicitly is what makes this
 * composable with any future multi-statement pruning logic (e.g. also
 * pruning old webhook delivery logs) added inside the same atomic block,
 * and guarantees that if the transaction throws, better-sqlite3 rolls back
 * everything it did — nothing here swallows that error.
 *
 * Returns the number of rows deleted.
 */
export function pruneOldEvents(db: Database.Database, retentionDays: number): number {
  const validation = validateRetentionDays(retentionDays);
  if (!validation.ok) {
    throw new Error(validation.error);
  }

  const deleteStmt = db.prepare(
    `DELETE FROM events WHERE created_at < datetime('now', '-' || ? || ' days')`
  );

  const pruneTransaction = db.transaction((days: number) => {
    const result = deleteStmt.run(days);
    return result.changes;
  });

  // better-sqlite3's transaction wrapper commits the callback's statements
  // together, or rolls all of them back if it throws — propagate any error
  // as-is so callers know pruning did not complete.
  return pruneTransaction(retentionDays);
}

/**
 * Runs SQLite's VACUUM command to reclaim disk space freed by prior
 * deletions.
 *
 * MUST NEVER be called from inside an active transaction — SQLite will
 * throw `cannot VACUUM from within a transaction` if you do. This is
 * exactly why `runVacuumCleanup` below calls `pruneOldEvents` (transactional)
 * to completion FIRST, and only then calls `runVacuum` (non-transactional)
 * as a separate, later step.
 */
export function runVacuum(db: Database.Database): void {
  db.exec("VACUUM");
}

/**
 * Orchestrates a full vacuum-cleanup cycle:
 *   1. Prune events older than the configured retention window, atomically.
 *   2. Only if pruning fully succeeds, reclaim disk space with VACUUM.
 *
 * If pruning throws, the error propagates immediately and VACUUM is never
 * invoked — we never want to reclaim disk space around data whose cleanup
 * step failed or rolled back ambiguously.
 */
export function runVacuumCleanup(
  db: Database.Database,
  options: VacuumCleanupOptions = {}
): VacuumCleanupResult {
  const retentionDays = options.retentionDays ?? DEFAULT_RETENTION_DAYS;

  logger.info("Starting sqlite vacuum cleanup", { retentionDays });

  // Step 1: transactional prune. If this throws, we intentionally do not
  // catch it here — propagate immediately and skip VACUUM entirely.
  const prunedEvents = pruneOldEvents(db, retentionDays);

  // Step 2: non-transactional VACUUM, only reached once pruning committed.
  runVacuum(db);

  logger.info("Completed sqlite vacuum cleanup", { prunedEvents });

  return { prunedEvents, vacuumed: true };
}

// ---------------------------------------------------------------------------
// Issue 1: Dynamic polling frequency intervals
// ---------------------------------------------------------------------------

export const DEFAULT_VACUUM_POLL_INTERVAL_MS = parseInt(
  process.env.VACUUM_POLL_INTERVAL_MS || "3600000", // 1 hour
  10,
);
export const MIN_VACUUM_POLL_INTERVAL_MS = parseInt(
  process.env.VACUUM_MIN_POLL_INTERVAL_MS || "300000", // 5 minutes
  10,
);
export const MAX_VACUUM_POLL_INTERVAL_MS = parseInt(
  process.env.VACUUM_MAX_POLL_INTERVAL_MS || "86400000", // 24 hours
  10,
);
/** Multiplier applied to the interval each idle cycle (no rows pruned). */
export const VACUUM_IDLE_BACKOFF_FACTOR = 2;
/** Consecutive idle cycles required before backing off. */
export const VACUUM_IDLE_THRESHOLD_CYCLES = 2;

export interface VacuumPollingState {
  currentIntervalMs: number;
  idleCycles: number;
  lastAdjustedAt: number;
}

let vacuumPollingState: VacuumPollingState = {
  currentIntervalMs: DEFAULT_VACUUM_POLL_INTERVAL_MS,
  idleCycles: 0,
  lastAdjustedAt: Date.now(),
};

/** Returns a read-only snapshot of the current vacuum polling state. */
export function getVacuumPollingState(): VacuumPollingState {
  return { ...vacuumPollingState };
}

/** Resets polling state to defaults. Useful for tests. */
export function resetVacuumPollingState(): void {
  vacuumPollingState = {
    currentIntervalMs: DEFAULT_VACUUM_POLL_INTERVAL_MS,
    idleCycles: 0,
    lastAdjustedAt: Date.now(),
  };
}

/**
 * Adjusts the vacuum polling interval based on how many rows were pruned in
 * the last cycle.
 *
 * - If `prunedRows === 0` for at least VACUUM_IDLE_THRESHOLD_CYCLES
 *   consecutive cycles, the interval doubles (up to MAX_VACUUM_POLL_INTERVAL_MS).
 * - If rows were pruned (the DB is active), the interval is reset to its
 *   minimum so the cleaner stays responsive.
 *
 * @param prunedRows - Number of rows deleted in the most recent vacuum cycle.
 * @returns Updated polling state snapshot.
 */
export function adjustVacuumPollingInterval(
  prunedRows: number,
): VacuumPollingState {
  const state = vacuumPollingState;

  if (prunedRows === 0) {
    state.idleCycles += 1;
    if (state.idleCycles >= VACUUM_IDLE_THRESHOLD_CYCLES) {
      state.currentIntervalMs = Math.min(
        state.currentIntervalMs * VACUUM_IDLE_BACKOFF_FACTOR,
        MAX_VACUUM_POLL_INTERVAL_MS,
      );
    }
  } else {
    // Active pruning — shrink back to minimum so we stay on top of growth.
    state.idleCycles = 0;
    state.currentIntervalMs = MIN_VACUUM_POLL_INTERVAL_MS;
  }

  state.lastAdjustedAt = Date.now();
  return { ...state };
}

// ---------------------------------------------------------------------------
// Issue 3: Dynamic start/end ledger range support
// ---------------------------------------------------------------------------

export interface LedgerRangePruneOptions {
  /** Inclusive lower bound of the ledger range to prune. */
  startLedger: number;
  /** Inclusive upper bound of the ledger range to prune. */
  endLedger: number;
}

export interface LedgerRangePruneResult {
  prunedEvents: number;
  startLedger: number;
  endLedger: number;
}

export const LEDGER_RANGE_ERROR_CODES = {
  INVALID_LEDGER_RANGE: "VACUUM_INVALID_LEDGER_RANGE",
} as const;

/**
 * Validates a ledger range.  Both values must be non-negative integers and
 * startLedger must be ≤ endLedger.
 */
export function validateLedgerRange(
  startLedger: number,
  endLedger: number,
):
  | { ok: true }
  | { ok: false; error: string; code: "VACUUM_INVALID_LEDGER_RANGE" } {
  if (
    typeof startLedger !== "number" ||
    !Number.isInteger(startLedger) ||
    startLedger < 0
  ) {
    return {
      ok: false,
      error: `startLedger must be a non-negative integer, got: ${startLedger}`,
      code: LEDGER_RANGE_ERROR_CODES.INVALID_LEDGER_RANGE,
    };
  }
  if (
    typeof endLedger !== "number" ||
    !Number.isInteger(endLedger) ||
    endLedger < 0
  ) {
    return {
      ok: false,
      error: `endLedger must be a non-negative integer, got: ${endLedger}`,
      code: LEDGER_RANGE_ERROR_CODES.INVALID_LEDGER_RANGE,
    };
  }
  if (startLedger > endLedger) {
    return {
      ok: false,
      error: `startLedger (${startLedger}) must be ≤ endLedger (${endLedger})`,
      code: LEDGER_RANGE_ERROR_CODES.INVALID_LEDGER_RANGE,
    };
  }
  return { ok: true };
}

/**
 * Deletes events whose `ledger_sequence` falls within [startLedger, endLedger]
 * (both inclusive).  Runs inside a transaction for atomicity — if anything
 * fails the deletion rolls back and no rows are partially removed.
 *
 * Returns the number of rows deleted.
 */
export function pruneEventsInLedgerRange(
  db: Database.Database,
  options: LedgerRangePruneOptions,
): LedgerRangePruneResult {
  const { startLedger, endLedger } = options;

  const validation = validateLedgerRange(startLedger, endLedger);
  if (!validation.ok) {
    throw new Error(validation.error);
  }

  const deleteStmt = db.prepare(
    `DELETE FROM events WHERE ledger_sequence >= ? AND ledger_sequence <= ?`,
  );

  const tx = db.transaction(() => {
    const result = deleteStmt.run(startLedger, endLedger);
    return result.changes;
  });

  const prunedEvents = tx() as number;

  logger.info("Pruned events in ledger range", {
    startLedger,
    endLedger,
    prunedEvents,
  });

  return { prunedEvents, startLedger, endLedger };
}

// ---------------------------------------------------------------------------
// Issue 4: Schema migration check utilities
// ---------------------------------------------------------------------------

export interface VacuumSchemaValidationResult {
  valid: boolean;
  missingTables: string[];
  missingColumns: Record<string, string[]>;
  errors: string[];
}

/**
 * Tables and columns that the vacuum cleaner requires to operate correctly.
 */
const VACUUM_REQUIRED_SCHEMA: Record<string, string[]> = {
  events: [
    "id",
    "contract_id",
    "event_type",
    "ledger_sequence",
    "timestamp",
    "data_json",
    "created_at",
  ],
  schema_migrations: ["version", "description", "applied_at"],
};

/**
 * Validates that the database schema contains all tables and columns needed
 * by the vacuum cleaner.  Does NOT run migrations — it only checks.
 *
 * Returns a detailed result so callers can surface specific problems.
 */
export function validateVacuumSchema(
  db: Database.Database,
): VacuumSchemaValidationResult {
  const missingTables: string[] = [];
  const missingColumns: Record<string, string[]> = {};
  const errors: string[] = [];

  for (const [tableName, requiredCols] of Object.entries(
    VACUUM_REQUIRED_SCHEMA,
  )) {
    const tableRow = db
      .prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
      )
      .get(tableName);

    if (!tableRow) {
      missingTables.push(tableName);
      continue;
    }

    const columns = db
      .prepare(`PRAGMA table_info(${tableName})`)
      .all() as Array<{ name: string }>;
    const columnNames = new Set(columns.map((c) => c.name));

    const missing = requiredCols.filter((col) => !columnNames.has(col));
    if (missing.length > 0) {
      missingColumns[tableName] = missing;
    }
  }

  const valid =
    missingTables.length === 0 && Object.keys(missingColumns).length === 0;

  return { valid, missingTables, missingColumns, errors };
}

/**
 * Throws if the database schema is not ready for the vacuum cleaner.
 * Call this at startup before scheduling any vacuum runs so the process
 * fails fast with a clear message rather than hitting obscure SQL errors.
 */
export function assertVacuumSchemaValid(db: Database.Database): void {
  const result = validateVacuumSchema(db);
  if (!result.valid) {
    const reasons = [
      ...result.missingTables.map((t) => `missing table: ${t}`),
      ...Object.entries(result.missingColumns).map(
        ([t, cols]) => `missing columns in ${t}: ${cols.join(", ")}`,
      ),
      ...result.errors,
    ];
    throw new Error(
      `Vacuum cleaner cannot start — database schema is out of sync: ${reasons.join("; ")}`,
    );
  }
}

// ---------------------------------------------------------------------------
// Exponential backoff retry (#343)
// ---------------------------------------------------------------------------

/** Retry configuration for vacuum operations subject to connection dropouts. */
export interface VacuumRetryConfig {
  /** Maximum number of retry attempts after the initial failure (default: 5). */
  maxRetries: number;
  /** Initial delay in ms after the first failure (default: 1000). */
  initialBackoffMs: number;
  /** Multiplier applied to the backoff on each consecutive failure (default: 2). */
  backoffMultiplier: number;
  /** Ceiling delay in ms (default: 30000). */
  maxBackoffMs: number;
}

/** Default retry configuration for vacuum connection dropouts. */
export const DEFAULT_VACUUM_RETRY_CONFIG: VacuumRetryConfig = {
  maxRetries: 5,
  initialBackoffMs: 1000,
  backoffMultiplier: 2,
  maxBackoffMs: 30_000,
};

/** Error-string fragments that indicate a transient, retryable failure. */
export const VACUUM_RETRYABLE_PATTERNS = [
  "timeout",
  "SQLITE_BUSY",
  "SQLITE_LOCKED",
  "database is locked",
  "database is busy",
  "ECONNRESET",
  "ECONNREFUSED",
  "ETIMEDOUT",
  "socket hang up",
  "connect timeout",
  "connection reset",
  "connection refused",
  "connection dropped",
  "RPC connection",
] as const;

/**
 * Returns true when `err` looks like a transient RPC connection timeout or
 * SQLite lock that is worth retrying.
 */
export function isVacuumRetryableError(err: unknown): boolean {
  if (!(err instanceof Error)) return false;
  const msg = err.message.toLowerCase();
  return (VACUUM_RETRYABLE_PATTERNS as readonly string[]).some((p) =>
    msg.includes(p.toLowerCase()),
  );
}

/**
 * Compute the backoff delay (ms) for a given attempt index.
 * attempt 0 → initialBackoffMs, attempt 1 → initialBackoffMs * multiplier, etc.,
 * capped at maxBackoffMs.
 */
export function computeVacuumBackoffMs(
  attempt: number,
  config: Pick<VacuumRetryConfig, "initialBackoffMs" | "backoffMultiplier" | "maxBackoffMs">,
): number {
  return Math.min(
    config.initialBackoffMs * Math.pow(config.backoffMultiplier, attempt),
    config.maxBackoffMs,
  );
}

function sleepSync(ms: number): void {
  if (ms <= 0) return;
  const sab = new SharedArrayBuffer(4);
  const ia = new Int32Array(sab);
  Atomics.wait(ia, 0, 0, ms);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Execute a synchronous vacuum operation with exponential backoff retry.
 * Uses `sleepSync` (Atomics.wait) so the delay is truly blocking — appropriate
 * for the synchronous better-sqlite3 calls used throughout this module.
 *
 * Only errors flagged by `isVacuumRetryableError` are retried; all other errors
 * propagate immediately. After `maxRetries` consecutive retries have been
 * exhausted the last error is rethrown.
 */
export function withVacuumRetrySync<T>(
  fn: () => T,
  config: Partial<VacuumRetryConfig> = {},
  context: string = "sqlite_vacuum_cleaner",
): T {
  const cfg = { ...DEFAULT_VACUUM_RETRY_CONFIG, ...config };
  let lastError: Error | null = null;

  for (let attempt = 0; attempt <= cfg.maxRetries; attempt++) {
    try {
      return fn();
    } catch (err) {
      lastError = err instanceof Error ? err : new Error(String(err));

      if (!isVacuumRetryableError(lastError) || attempt >= cfg.maxRetries) {
        throw lastError;
      }

      const delay = computeVacuumBackoffMs(attempt, cfg);
      logger.warn(`${context} failed, retrying`, {
        attempt: attempt + 1,
        maxRetries: cfg.maxRetries,
        backoffMs: delay,
        error: lastError.message,
      });
      sleepSync(delay);
    }
  }

  throw lastError ?? new Error(`${context} failed after retries`);
}

/**
 * Async variant of vacuum retry for callers that prefer Promise-based backoff.
 * Otherwise identical semantics to `withVacuumRetrySync`.
 */
export async function withVacuumRetry<T>(
  fn: () => Promise<T> | T,
  config: Partial<VacuumRetryConfig> = {},
  context: string = "sqlite_vacuum_cleaner",
): Promise<T> {
  const cfg = { ...DEFAULT_VACUUM_RETRY_CONFIG, ...config };
  let lastError: Error | null = null;

  for (let attempt = 0; attempt <= cfg.maxRetries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastError = err instanceof Error ? err : new Error(String(err));

      if (!isVacuumRetryableError(lastError) || attempt >= cfg.maxRetries) {
        throw lastError;
      }

      const delay = computeVacuumBackoffMs(attempt, cfg);
      logger.warn(`${context} failed, retrying`, {
        attempt: attempt + 1,
        maxRetries: cfg.maxRetries,
        backoffMs: delay,
        error: lastError.message,
      });
      await sleep(delay);
    }
  }

  throw lastError ?? new Error(`${context} failed after retries`);
}

/**
 * Run `runVacuum` wrapped in exponential backoff retry so transient RPC
 * connection timeouts and SQLite lock dropouts don't abort the cycle.
 *
 * Pruning is intentionally NOT retried here — it runs inside a transaction
 * that either commits or rolls back atomically. Only the separate VACUUM step
 * (which is non-transactional and most susceptible to "database is locked"
 * errors from concurrent writers) gets the retry treatment.
 */
export function runVacuumWithRetry(
  db: Database.Database,
  retryConfig: Partial<VacuumRetryConfig> = {},
): void {
  withVacuumRetrySync(
    () => runVacuum(db),
    retryConfig,
    "sqlite_vacuum_cleaner.run_vacuum",
  );
}

/**
 * Run a full vacuum-cleanup cycle with retry on the VACUUM step.
 *
 * Pruning runs once (no retry — a failed prune rolls back atomically and
 * should not be blindly retried). The VACUUM step is retried with
 * exponential backoff for transient connection dropouts.
 */
export function runVacuumCleanupWithRetry(
  db: Database.Database,
  options: VacuumCleanupOptions = {},
  retryConfig: Partial<VacuumRetryConfig> = {},
): VacuumCleanupResult {
  const retentionDays = options.retentionDays ?? DEFAULT_RETENTION_DAYS;

  logger.info("Starting sqlite vacuum cleanup (with retry)", {
    retentionDays,
  });

  const prunedEvents = pruneOldEvents(db, retentionDays);

  runVacuumWithRetry(db, retryConfig);

  logger.info("Completed sqlite vacuum cleanup (with retry)", { prunedEvents });

  return { prunedEvents, vacuumed: true };
}
