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
//   - Polling diagnostics logs (Issue 5): every pruning step, the VACUUM
//     command, and the whole cleanup cycle emit a debug log whose message
//     carries `elapsedMs=` so operators can spot slow cleanup runs without
//     enabling a profiler, mirroring indexer_runner / indexer_metrics_collector
//     (#346).
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

// ---------------------------------------------------------------------------
// Concurrency lock (#345)
// ---------------------------------------------------------------------------
//
// SQLite does NOT allow VACUUM inside an explicit transaction, so two
// concurrent `runVacuumCleanup` calls could race: one starts pruning while
// the other tries to VACUUM, or both attempt VACUUM simultaneously and one
// fails with "cannot VACUUM from within a transaction" or "database is locked".
//
// A simple promise-based mutex serialises entry into the critical section.
// Only one cleanup cycle runs at a time; callers that arrive while a cycle is
// in progress simply wait for it to finish instead of overlapping.

let vacuumLock: Promise<void> = Promise.resolve();
let vacuumLockActive = false;

export function isVacuumLocked(): boolean {
  return vacuumLockActive;
}

export const ERROR_CODES = {
  INVALID_RETENTION: "VACUUM_INVALID_RETENTION",
} as const;

export type ErrorCode = (typeof ERROR_CODES)[keyof typeof ERROR_CODES];

// ---------------------------------------------------------------------------
// Issue 5: Polling diagnostics logs (#346)
// ---------------------------------------------------------------------------

const VACUUM_COMPONENT_NAME = "sqlite_vacuum_cleaner";

export interface VacuumPollDiagnostics {
  component: string;
  operation: string;
  status: "started" | "success" | "failure";
  /** Wall-clock duration of the operation in milliseconds. */
  elapsedMs: number;
  /** Number of event rows deleted by a pruning step. */
  prunedEvents?: number;
  retentionDays?: number;
  startLedger?: number;
  endLedger?: number;
  error?: string;
}

/** Round to microsecond precision so sub-millisecond operations stay readable. */
function roundVacuumElapsed(elapsedMs: number): number {
  return Math.round(Math.max(0, elapsedMs) * 1000) / 1000;
}

/**
 * Emit a sqlite_vacuum_cleaner diagnostics debug log.
 *
 * The message string always carries `elapsedMs=` (plus `prunedEvents=` when a
 * pruning step ran) so log-scraping validation can assert timing values are
 * present; the same values are repeated in the structured meta object for log
 * processors.
 */
export function logVacuumPollDiagnostics(
  diagnostics: VacuumPollDiagnostics,
): void {
  const parts = [
    `${diagnostics.component} poll diagnostics`,
    `operation=${diagnostics.operation}`,
    `status=${diagnostics.status}`,
    `elapsedMs=${diagnostics.elapsedMs}`,
  ];
  if (diagnostics.prunedEvents !== undefined) {
    parts.push(`prunedEvents=${diagnostics.prunedEvents}`);
  }
  if (diagnostics.retentionDays !== undefined) {
    parts.push(`retentionDays=${diagnostics.retentionDays}`);
  }
  if (diagnostics.startLedger !== undefined) {
    parts.push(`startLedger=${diagnostics.startLedger}`);
  }
  if (diagnostics.endLedger !== undefined) {
    parts.push(`endLedger=${diagnostics.endLedger}`);
  }
  logger.debug(parts.join(" "), diagnostics);
}

/**
 * Time a vacuum operation and emit its diagnostics. A numeric result is
 * reported as `prunedEvents`; failures are logged with the elapsed time and
 * the error before being rethrown for the caller to handle as before.
 */
function timeVacuumOperation<T>(
  operation: string,
  details: Omit<
    VacuumPollDiagnostics,
    "component" | "operation" | "status" | "elapsedMs" | "error"
  >,
  fn: () => T,
): T {
  const startedAt = performance.now();
  try {
    const result = fn();
    logVacuumPollDiagnostics({
      component: VACUUM_COMPONENT_NAME,
      operation,
      status: "success",
      elapsedMs: roundVacuumElapsed(performance.now() - startedAt),
      prunedEvents: typeof result === "number" ? result : undefined,
      ...details,
    });
    return result;
  } catch (err) {
    logVacuumPollDiagnostics({
      component: VACUUM_COMPONENT_NAME,
      operation,
      status: "failure",
      elapsedMs: roundVacuumElapsed(performance.now() - startedAt),
      error: err instanceof Error ? err.message : String(err),
      ...details,
    });
    throw err;
  }
}

/** Extract a message from an unknown thrown value. */
function vacuumErrorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

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

  // The DELETE statement and its transaction wrapper are timed together so a
  // prepare/execution failure is attributed to the prune_old_events stage and
  // emitted as a failure diagnostic before propagating.
  const pruneTransaction = (days: number): number => {
    const deleteStmt = db.prepare(
      `DELETE FROM events WHERE created_at < datetime('now', '-' || ? || ' days')`
    );
    const tx = db.transaction((d: number) => {
      const result = deleteStmt.run(d);
      return result.changes;
    });
    return tx(days);
  };

  return timeVacuumOperation(
    "prune_old_events",
    { retentionDays },
    () => pruneTransaction(retentionDays),
  );
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
  timeVacuumOperation("run_vacuum", {}, () => db.exec("VACUUM"));
}

/**
 * Orchestrates a full vacuum-cleanup cycle:
 *   1. Prune events older than the configured retention window, atomically.
 *   2. Only if pruning fully succeeds, reclaim disk space with VACUUM.
 *
 * If pruning throws, the error propagates immediately and VACUUM is never
 * invoked — we never want to reclaim disk space around data whose cleanup
 * step failed or rolled back ambiguously.
 *
 * Concurrency guard (#345): multiple concurrent calls are serialised via
 * an in-memory promise lock so two cleanup cycles never overlap.  Callers
 * that arrive while a cycle is in progress wait for it to finish rather
 * than racing on the database.
 */
export function runVacuumCleanup(
  db: Database.Database,
  options: VacuumCleanupOptions = {}
): VacuumCleanupResult {
  const retentionDays = options.retentionDays ?? DEFAULT_RETENTION_DAYS;

  const startedAt = performance.now();

  logger.info("Starting sqlite vacuum cleanup", { retentionDays });

  logVacuumPollDiagnostics({
    component: VACUUM_COMPONENT_NAME,
    operation: "vacuum_cleanup",
    status: "started",
    elapsedMs: 0,
    retentionDays,
  });

  try {
    // Step 1: transactional prune. If this throws, we intentionally do not
    // re-catch it beyond the failure diagnostic — propagate immediately and
    // skip VACUUM entirely.
    const prunedEvents = pruneOldEvents(db, retentionDays);

    // Step 2: non-transactional VACUUM, only reached once pruning committed.
    runVacuum(db);

    logger.info("Completed sqlite vacuum cleanup", { prunedEvents });

    logVacuumPollDiagnostics({
      component: VACUUM_COMPONENT_NAME,
      operation: "vacuum_cleanup",
      status: "success",
      elapsedMs: roundVacuumElapsed(performance.now() - startedAt),
      retentionDays,
      prunedEvents,
    });

    return { prunedEvents, vacuumed: true };
  } catch (err) {
    logVacuumPollDiagnostics({
      component: VACUUM_COMPONENT_NAME,
      operation: "vacuum_cleanup",
      status: "failure",
      elapsedMs: roundVacuumElapsed(performance.now() - startedAt),
      retentionDays,
      error: vacuumErrorMessage(err),
    });
    throw err;
  }
}

/**
 * Async variant of runVacuumCleanup that honours the concurrency lock.
 * Internally chains onto the shared `vacuumLock` promise so at most one
 * cleanup cycle is in flight at any time.
 */
export async function runVacuumCleanupConcurrent(
  db: Database.Database,
  options: VacuumCleanupOptions = {}
): Promise<VacuumCleanupResult> {
  // Chain onto the existing lock so only one cycle runs at a time.
  const previous = vacuumLock;
  let release: () => void;
  vacuumLock = new Promise<void>((resolve) => {
    release = resolve;
  });
  vacuumLockActive = true;

  await previous;

  try {
    return runVacuumCleanup(db, options);
  } finally {
    vacuumLockActive = false;
    release!();
  }
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

  const prunedEvents = timeVacuumOperation(
    "prune_ledger_range",
    { startLedger, endLedger },
    tx,
  ) as number;

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
// Issue 5: SQLite index structures for vacuum cleaner lookups (#344)
// ---------------------------------------------------------------------------

/**
 * Index names backing the vacuum cleaner's row lookups.
 *
 * The cleaner finds stale rows through two predicates:
 *   1. retention-pruning by `created_at`  → covered by idx_events_created_at
 *      (and idx_events_created_at_ledger for the composite layout).
 *   2. ledger-range pruning by `ledger_sequence` → covered by
 *      idx_events_ledger_sequence (and idx_events_ledger_created_at).
 *
 * Exporting the names lets tests run EXPLAIN QUERY PLAN and assert the indexes
 * are actually used, and lets operators inspect the schema (#344).
 */
export const VACUUM_CLEANER_INDEXES = {
  eventsCreatedAt: "idx_events_created_at",
  eventsLedgerSequence: "idx_events_ledger_sequence",
  eventsCreatedAtLedger: "idx_events_created_at_ledger",
  eventsLedgerCreatedAt: "idx_events_ledger_created_at",
} as const;

/** All index names managed by the vacuum cleaner. */
export function getVacuumIndexNames(): string[] {
  return Object.values(VACUUM_CLEANER_INDEXES);
}

/**
 * Ensure every index the vacuum cleaner relies on exists, creating any missing
 * ones idempotently. Used alongside the schema manager migrations so the cleaner
 * can self-heal a database that predates migration 6 (#344).
 *
 * @returns The names of the indexes that are now present.
 */
export function ensureVacuumIndexes(db: Database.Database): string[] {
  db.exec(
    `
    CREATE INDEX IF NOT EXISTS ${VACUUM_CLEANER_INDEXES.eventsCreatedAt}
      ON events (created_at);

    CREATE INDEX IF NOT EXISTS ${VACUUM_CLEANER_INDEXES.eventsLedgerSequence}
      ON events (ledger_sequence);

    CREATE INDEX IF NOT EXISTS ${VACUUM_CLEANER_INDEXES.eventsCreatedAtLedger}
      ON events (created_at, ledger_sequence);

    CREATE INDEX IF NOT EXISTS ${VACUUM_CLEANER_INDEXES.eventsLedgerCreatedAt}
      ON events (ledger_sequence, created_at);
    `,
  );
  return getVacuumIndexNames();
}

/**
 * Run SQLite's EXPLAIN QUERY PLAN for a parameterized statement against the
 * given connection. Mirrors ledger_range_tracker's helper so tests can assert
 * the vacuum cleaner's indexes are utilized for lookups (#344).
 */
export function vacuumExplainQueryPlan(
  db: Database.Database,
  sql: string,
  ...params: unknown[]
): Array<Record<string, unknown>> {
  return db.prepare(`EXPLAIN QUERY PLAN ${sql}`).all(...params) as Array<
    Record<string, unknown>
  >;
}

/**
 * True when any EXPLAIN QUERY PLAN detail row references the expected index
 * name. Mirrors ledger_range_tracker's helper (#344).
 */
export function vacuumQueryPlanUsesIndex(
  plan: Array<Record<string, unknown>>,
  indexName: string,
): boolean {
  return plan.some((row) =>
    Object.values(row).some(
      (value) => typeof value === "string" && value.includes(indexName),
    ),
  );
}
