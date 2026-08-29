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
  const monitor = defaultVacuumFailureMonitor;

  monitor.checkStall();

  logger.info("Starting sqlite vacuum cleanup", { retentionDays });

  // Step 1: transactional prune. If this throws, record the failure (which
  // alerts once the consecutive-failure threshold is reached) and propagate
  // immediately — VACUUM is intentionally skipped.
  let prunedEvents: number;
  try {
    prunedEvents = pruneOldEvents(db, retentionDays);
  } catch (err) {
    monitor.recordFailure("prune", {
      error: err instanceof Error ? err.message : String(err),
    });
    throw err;
  }

  // Step 2: non-transactional VACUUM, only reached once pruning committed.
  try {
    runVacuum(db);
  } catch (err) {
    monitor.recordFailure("vacuum", {
      error: err instanceof Error ? err.message : String(err),
    });
    throw err;
  }

  monitor.recordSuccess();

  logger.info("Completed sqlite vacuum cleanup", { prunedEvents });

  return { prunedEvents, vacuumed: true };
}

// ---------------------------------------------------------------------------
// Failure alerting (#347)
// ---------------------------------------------------------------------------

/** Consecutive cleanup failures before a warning alert is raised. */
export const DEFAULT_VACUUM_FAILURE_THRESHOLD = 3;

/**
 * Elapsed ms without a successful cleanup before a stall alert is raised.
 * Defaults to twice the default vacuum polling interval (1 hour), so a
 * missed cycle or two doesn't immediately alert.
 */
export const DEFAULT_VACUUM_STALL_THRESHOLD_MS = 2 * 60 * 60 * 1000; // 2 hours

export type VacuumFailureType = "prune" | "vacuum" | "stall";

function readPositiveIntEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 1) {
    logger.warn("sqlite_vacuum_cleaner ignoring invalid threshold config", {
      variable: name,
      received: raw,
      fallback,
    });
    return fallback;
  }
  return value;
}

/**
 * Reads alert thresholds from `VACUUM_FAILURE_THRESHOLD` and
 * `VACUUM_STALL_THRESHOLD_MS`. Invalid values fall back to the defaults with
 * a warning rather than throwing — telemetry config must not take the
 * cleaner down.
 */
export function getVacuumAlertConfig(): {
  failureThreshold: number;
  stallThresholdMs: number;
} {
  return {
    failureThreshold: readPositiveIntEnv(
      "VACUUM_FAILURE_THRESHOLD",
      DEFAULT_VACUUM_FAILURE_THRESHOLD,
    ),
    stallThresholdMs: readPositiveIntEnv(
      "VACUUM_STALL_THRESHOLD_MS",
      DEFAULT_VACUUM_STALL_THRESHOLD_MS,
    ),
  };
}

export interface VacuumFailureMonitorOptions {
  failureThreshold?: number;
  stallThresholdMs?: number;
}

/**
 * Tracks consecutive vacuum-cleanup failures and cleanup stalls, raising a
 * warning alert once the configured counts are reached (#347).
 *
 * Every failure is logged as an error; the warning alert fires from the
 * threshold onwards so a persistent outage keeps surfacing rather than
 * alerting once and going quiet. A successful cleanup clears the state.
 */
export class VacuumFailureMonitor {
  readonly failureThreshold: number;
  readonly stallThresholdMs: number;

  private consecutiveFailures = 0;
  private lastSuccessfulAt: number | null = null;
  private alertActive = false;

  constructor(options: VacuumFailureMonitorOptions = {}) {
    const env = getVacuumAlertConfig();
    this.failureThreshold = options.failureThreshold ?? env.failureThreshold;
    this.stallThresholdMs = options.stallThresholdMs ?? env.stallThresholdMs;
  }

  getConsecutiveFailures(): number {
    return this.consecutiveFailures;
  }

  getLastSuccessfulAt(): number | null {
    return this.lastSuccessfulAt;
  }

  isAlertActive(): boolean {
    return this.alertActive;
  }

  /**
   * Record a failed cleanup step. Returns the new consecutive-failure count.
   * Emits an error every time and a warning alert from the threshold onwards.
   */
  recordFailure(
    failureType: VacuumFailureType,
    details: { error?: string } = {},
  ): number {
    this.consecutiveFailures += 1;

    const payload = {
      failureType,
      consecutiveFailures: this.consecutiveFailures,
      threshold: this.failureThreshold,
      error: details.error,
    };

    logger.error("sqlite_vacuum_cleaner operation failed", payload);

    if (this.consecutiveFailures >= this.failureThreshold) {
      this.alertActive = true;
      logger.warn(
        "sqlite_vacuum_cleaner alert: consecutive failure threshold reached",
        {
          ...payload,
          action:
            "Inspect the sqlite database and disk health; alerting clears automatically after the next successful cleanup.",
        },
      );
    }

    return this.consecutiveFailures;
  }

  /** Record a successful cleanup, clearing any active alert. */
  recordSuccess(): void {
    const hadFailures = this.consecutiveFailures > 0 || this.alertActive;
    this.consecutiveFailures = 0;
    this.lastSuccessfulAt = Date.now();
    if (hadFailures) {
      logger.info("sqlite_vacuum_cleaner recovered after failures", {});
    }
    this.alertActive = false;
  }

  /**
   * Warn when no successful cleanup has landed inside the stall window.
   * Does not touch the consecutive-failure counter, so a later success still
   * recovers cleanly. Returns true when a stall was reported.
   */
  checkStall(): boolean {
    if (this.lastSuccessfulAt === null) return false;
    const elapsedMs = Date.now() - this.lastSuccessfulAt;
    if (elapsedMs <= this.stallThresholdMs) return false;

    logger.warn("sqlite_vacuum_cleaner alert: stall threshold reached", {
      failureType: "stall" as const,
      consecutiveFailures: this.consecutiveFailures,
      threshold: this.failureThreshold,
      stallThresholdMs: this.stallThresholdMs,
      elapsedMs,
      action:
        "No successful vacuum cleanup within the stall window; inspect the poller and database health.",
    });
    return true;
  }

  reset(): void {
    this.consecutiveFailures = 0;
    this.lastSuccessfulAt = null;
    this.alertActive = false;
  }
}

let defaultVacuumFailureMonitor = new VacuumFailureMonitor();

/** The monitor backing `runVacuumCleanup`. */
export function getVacuumFailureMonitor(): VacuumFailureMonitor {
  return defaultVacuumFailureMonitor;
}

/**
 * Clear vacuum cleaner alert state and re-read the threshold configuration.
 * Intended for tests and for reloads after a config change.
 */
export function resetVacuumFailureMonitorState(): void {
  defaultVacuumFailureMonitor = new VacuumFailureMonitor();
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
