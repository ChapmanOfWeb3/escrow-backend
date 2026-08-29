import type Database from "better-sqlite3";
import {
  getDb,
  getShippedMigrationVersions,
  verifySchemaIntegrity,
  verifySchemaUpToDate,
} from "./db.js";
import logger from "../utils/logger.js";

/**
 * DatabaseWriterPool manages concurrent database write operations with full transaction support.
 * Ensures that all write operations are atomic and consistent even under high load.
 *
 * Features:
 * - Atomic write-read consistency (ACID guarantees)
 * - Transaction isolation for concurrent writes
 * - Automatic rollback on failures
 * - Queue-based serialization to prevent writer contention
 * - Built-in retry logic for transient conflicts
 * - Migration verification hooks that validate the schema before starting (#331)
 * - High-frequency debug diagnostics for write speeds and payload sizes (#328)
 */

export interface WriteOperation<T> {
  execute: (db: ReturnType<typeof getDb>) => T;
  name?: string;
}

export interface WriteResult<T> {
  success: boolean;
  data?: T;
  error?: Error;
  retries: number;
  executionTimeMs: number;
}

/**
 * Queue for serializing write operations to prevent concurrent writer contention.
 * SQLite can only handle one writer at a time, so we queue writes to provide
 * consistent behavior and predictable throughput.
 */
const writeQueue: Array<{
  operation: WriteOperation<any>;
  resolve: (result: WriteResult<any>) => void;
  reject: (error: Error) => void;
}> = [];

let isProcessing = false;

/**
 * Process the write queue sequentially.
 * Each write is executed inside a transaction with automatic rollback on failure.
 */
async function processWriteQueue(): Promise<void> {
  if (isProcessing) return;
  isProcessing = true;

  const drainStartedAt = performance.now();
  let drainedCount = 0;

  try {
    while (writeQueue.length > 0) {
      drainedCount++;
      const item = writeQueue.shift();
      if (!item) break;

      const { operation, resolve, reject } = item;
      try {
        const result = await executeWrite(operation);
        resolve(result);
      } catch (error) {
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    }
  } finally {
    isProcessing = false;

    logWriterPoolDiagnostics({
      pool: POOL_NAME,
      operation: "drain_write_queue",
      status: "success",
      elapsedMs: roundElapsed(performance.now() - drainStartedAt),
      queueDepth: writeQueue.length,
      attempt: drainedCount,
    });

    // If new items were added while processing, process them
    if (writeQueue.length > 0) {
      processWriteQueue().catch((err) =>
        logger.error("Error processing write queue", {
          error: err instanceof Error ? err.message : String(err),
        })
      );
    }
  }
}

/**
 * Execute a single write operation inside a transaction.
 * Automatically retries on transient failures (e.g., database locked).
 *
 * @param operation The write operation to execute
 * @param maxRetries Maximum number of retry attempts
 * @returns WriteResult with success status, data, error, and metrics
 */
async function executeWrite<T>(
  operation: WriteOperation<T>,
  maxRetries: number = 3
): Promise<WriteResult<T>> {
  const db = getDb();
  const operationName = operation.name || "unknown";
  const startTime = Date.now();
  const startedAt = performance.now();
  let lastError: Error | null = null;
  let retryCount = 0;

  logWriterPoolDiagnostics({
    pool: POOL_NAME,
    operation: operationName,
    status: "started",
    elapsedMs: 0,
    queueDepth: writeQueue.length,
  });

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const result = await new Promise<T>((resolve, reject) => {
        try {
          const executeTransaction = db.transaction(() => {
            return operation.execute(db);
          });

          const data = executeTransaction();
          resolve(data);
        } catch (err) {
          reject(err);
        }
      });

      const executionTimeMs = Date.now() - startTime;

      if (attempt > 0) {
        logger.info("Write operation succeeded after retry", {
          operationName,
          retries: attempt,
          executionTimeMs,
        });
      }

      logWriterPoolDiagnostics({
        pool: POOL_NAME,
        operation: operationName,
        status: "success",
        elapsedMs: roundElapsed(performance.now() - startedAt),
        payloadSizeBytes: writerPoolPayloadSizeBytes(result),
        queueDepth: writeQueue.length,
        attempt: attempt + 1,
        retries: attempt,
      });

      return {
        success: true,
        data: result,
        retries: attempt,
        executionTimeMs,
      };
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      retryCount = attempt;

      // Check if error is retryable (database locked)
      const isRetryable =
        lastError.message.includes("database is locked") ||
        lastError.message.includes("SQLITE_BUSY");

      if (attempt < maxRetries && isRetryable) {
        // Exponential backoff: 10ms, 50ms, 250ms
        const backoffMs = Math.min(10 * Math.pow(5, attempt), 1000);
        logger.debug("Write operation failed, retrying", {
          operationName,
          attempt: attempt + 1,
          maxRetries,
          error: lastError.message,
          backoffMs,
        });
        logWriterPoolDiagnostics({
          pool: POOL_NAME,
          operation: operationName,
          status: "retry",
          elapsedMs: roundElapsed(performance.now() - startedAt),
          queueDepth: writeQueue.length,
          attempt: attempt + 1,
          error: lastError.message,
        });

        await new Promise((resolve) => setTimeout(resolve, backoffMs));
        continue;
      }

      // Non-retryable error or max retries exceeded
      const executionTimeMs = Date.now() - startTime;

      logger.error("Write operation failed", {
        operationName,
        retries: attempt,
        executionTimeMs,
        error: lastError.message,
      });
      logWriterPoolDiagnostics({
        pool: POOL_NAME,
        operation: operationName,
        status: "failure",
        elapsedMs: roundElapsed(performance.now() - startedAt),
        queueDepth: writeQueue.length,
        attempt: attempt + 1,
        retries: attempt,
        error: lastError.message,
      });

      return {
        success: false,
        error: lastError,
        retries: attempt,
        executionTimeMs,
      };
    }
  }

  // Should not reach here, but handle just in case
  const executionTimeMs = Date.now() - startTime;
  return {
    success: false,
    error: lastError || new Error("Unknown write failure"),
    retries: retryCount,
    executionTimeMs,
  };
}

/**
 * Queue a write operation for execution.
 * Operations are processed sequentially to prevent writer contention.
 *
 * @param operation The write operation to queue
 * @returns Promise that resolves with the WriteResult
 */
export function queueWrite<T>(operation: WriteOperation<T>): Promise<WriteResult<T>> {
  return new Promise((resolve, reject) => {
    // When a start was requested with enforcement, writes are refused until
    // the schema has been verified – a stale database must not be written to.
    if (enforceStart && !poolStarted) {
      const issues = lastSchemaReport?.issues ?? [
        "startWriterPool() has not completed successfully",
      ];
      reject(
        new WriterPoolSchemaError(
          `database_writer_pool is not started – refusing write "${operation.name || "unknown"}": ${issues.join("; ")}`,
          issues,
        ),
      );
      return;
    }

    writeQueue.push({ operation, resolve, reject });

    logWriterPoolDiagnostics({
      pool: POOL_NAME,
      operation: operation.name || "unknown",
      status: "started",
      elapsedMs: 0,
      queueDepth: writeQueue.length,
    });

    processWriteQueue().catch((err) => {
      logger.error("Error processing write queue", {
        error: err instanceof Error ? err.message : String(err),
      });
    });
  });
}

/**
 * Execute a write operation with transaction support and automatic rollback on failure.
 * Use this for critical operations that must succeed or be fully rolled back.
 *
 * @param operation The write operation to execute
 * @returns WriteResult with full details and metrics
 */
export async function executeWriteTransaction<T>(
  operation: WriteOperation<T>
): Promise<WriteResult<T>> {
  return queueWrite(operation);
}

/**
 * Execute multiple write operations as part of a single atomic transaction.
 * If any operation fails, all are rolled back.
 *
 * @param operations Array of write operations to execute atomically
 * @param name Optional name for the composite operation
 * @returns WriteResult containing an array of results for each operation
 */
export async function executeBatchWrite<T>(
  operations: WriteOperation<T>[],
  name?: string
): Promise<WriteResult<T[]>> {
  const compositeOperation: WriteOperation<T[]> = {
    name: name || `batch-${operations.length}-writes`,
    execute: (db) => {
      const results: T[] = [];

      for (const op of operations) {
        const executeTransaction = db.transaction(() => {
          return op.execute(db);
        });
        results.push(executeTransaction());
      }

      return results;
    },
  };

  return queueWrite(compositeOperation);
}

/**
 * Wait for all pending write operations to complete.
 * Useful for ensuring consistency before shutting down or reading critical data.
 *
 * @returns Promise that resolves when write queue is empty
 */
export async function flushWriteQueue(): Promise<void> {
  // Wait until queue is empty and not currently processing
  return new Promise((resolve) => {
    const check = () => {
      if (writeQueue.length === 0 && !isProcessing) {
        resolve();
      } else {
        setImmediate(check);
      }
    };
    check();
  });
}

/**
 * Get the current size of the write queue.
 * Useful for monitoring load and detecting bottlenecks.
 *
 * @returns Number of pending write operations
 */
export function getWriteQueueSize(): number {
  return writeQueue.length;
}

/**
 * Get the current processing state.
 * Returns true if a write operation is currently being processed.
 *
 * @returns Boolean indicating if queue is currently processing
 */
export function isWriteQueueProcessing(): boolean {
  return isProcessing;
}

/**
 * Helper to create a write operation for a single SQL update.
 * Useful for simple INSERT/UPDATE/DELETE operations.
 *
 * @param name Operation name for logging
 * @param statement SQL statement to execute
 * @param params Parameters for the statement
 * @returns WriteOperation that executes the statement
 */
export function createSqlOperation(
  name: string,
  statement: string,
  params: any[] = []
): WriteOperation<{ changes: number; lastInsertRowid: number | bigint }> {
  return {
    name,
    execute: (db) => {
      const stmt = db.prepare(statement);
      const result = stmt.run(...params);
      return {
        changes: result.changes,
        lastInsertRowid: result.lastInsertRowid,
      };
    },
  };
}

/**
 * Helper to create a write operation that reads-then-writes.
 * Ensures that the read and write happen consistently within the same transaction.
 *
 * @param name Operation name for logging
 * @param operation Function that reads, processes, and writes data
 * @returns WriteOperation that executes the operation atomically
 */
export function createReadWriteOperation<T>(
  name: string,
  operation: (db: ReturnType<typeof getDb>) => T
): WriteOperation<T> {
  return {
    name,
    execute: operation,
  };
}

// ---------------------------------------------------------------------------
// Polling diagnostics (#328)
// ---------------------------------------------------------------------------

const POOL_NAME = "database_writer_pool";

export interface WriterPoolDiagnostics {
  pool: string;
  operation: string;
  status: "started" | "success" | "failure" | "retry";
  /** Wall-clock duration of the operation in milliseconds. */
  elapsedMs: number;
  payloadSizeBytes?: number;
  queueDepth?: number;
  attempt?: number;
  retries?: number;
  error?: string;
}

/** Byte size of a JSON-serialized payload, matching indexer_runner's helper. */
export function writerPoolPayloadSizeBytes(value: unknown): number {
  try {
    return Buffer.byteLength(JSON.stringify(value ?? null), "utf8");
  } catch {
    // Circular or non-serializable results must not break a write.
    return 0;
  }
}

/** Round to microsecond precision so sub-millisecond writes stay readable. */
function roundElapsed(elapsedMs: number): number {
  return Math.round(Math.max(0, elapsedMs) * 1000) / 1000;
}

/**
 * Emit a database_writer_pool diagnostics debug log.
 *
 * The message string always carries `elapsedMs=` (and `payloadSizeBytes=` /
 * `queueDepth=` when known) so log-scraping validation can assert timing
 * values are present; the same values are repeated in the structured meta
 * object for log processors.
 */
export function logWriterPoolDiagnostics(
  diagnostics: WriterPoolDiagnostics,
): void {
  const parts = [
    `${diagnostics.pool} poll diagnostics`,
    `operation=${diagnostics.operation}`,
    `status=${diagnostics.status}`,
    `elapsedMs=${diagnostics.elapsedMs}`,
  ];
  if (diagnostics.payloadSizeBytes !== undefined) {
    parts.push(`payloadSizeBytes=${diagnostics.payloadSizeBytes}`);
  }
  if (diagnostics.queueDepth !== undefined) {
    parts.push(`queueDepth=${diagnostics.queueDepth}`);
  }
  if (diagnostics.attempt !== undefined) {
    parts.push(`attempt=${diagnostics.attempt}`);
  }
  logger.debug(parts.join(" "), diagnostics);
}

// ---------------------------------------------------------------------------
// Migration verification hooks (#331)
// ---------------------------------------------------------------------------

export class WriterPoolSchemaError extends Error {
  readonly issues: string[];

  constructor(message: string, issues: string[]) {
    super(message);
    this.name = "WriterPoolSchemaError";
    this.issues = issues;
  }
}

export interface WriterPoolSchemaReport {
  valid: boolean;
  /** Migration versions recorded as applied. */
  appliedVersions: number[];
  /** Migration versions the code ships but the database has not applied. */
  missingVersions: number[];
  /** Human-readable reasons the schema is considered out of sync. */
  issues: string[];
}

/**
 * A custom check run during `startWriterPool`. Return one or more issue
 * strings to fail the start, or nothing/an empty array to pass. A hook that
 * throws is reported as an issue rather than escaping the start call.
 */
export type MigrationVerificationHook = (
  db: ReturnType<typeof getDb>,
) => string[] | string | void;

const migrationHooks = new Map<string, MigrationVerificationHook>();

/**
 * Register a named schema check run by `startWriterPool` after the built-in
 * verification. Registering the same name twice replaces the earlier hook.
 */
export function registerMigrationVerificationHook(
  name: string,
  hook: MigrationVerificationHook,
): void {
  migrationHooks.set(name, hook);
}

export function unregisterMigrationVerificationHook(name: string): boolean {
  return migrationHooks.delete(name);
}

export function clearMigrationVerificationHooks(): void {
  migrationHooks.clear();
}

export function getMigrationVerificationHookNames(): string[] {
  return [...migrationHooks.keys()];
}

// ---------------------------------------------------------------------------
// SQLite index structures for write-path lookups (#326)
// ---------------------------------------------------------------------------
//
// The pool serializes writes against the shared indexer schema. The indexes
// below cover the lookup / filter / uniqueness patterns those writes actually
// use (keyed UPDATE/DELETE, INSERT OR IGNORE conflict checks, read-then-write
// existence probes). Unique constraints already provide covering indexes for
// several of those paths; they are listed separately so we do not create
// redundant secondary indexes that would only slow the write-heavy queue.

/** Named indexes the writer pool's lookups depend on. */
export const WRITER_POOL_INDEXES = {
  eventContractLedger: "idx_events_contract_ledger",
  webhookByContract: "idx_webhook_subscriptions_contract",
  webhookByUrl: "idx_webhook_subscriptions_webhook_url",
  activeContracts: "idx_monitored_contracts_active",
} as const;

/**
 * Unique / primary-key indexes created by table constraints. These already
 * cover equality lookups; adding a second B-tree on the same columns would
 * be redundant and would tax every INSERT/UPDATE/DELETE.
 */
export const WRITER_POOL_UNIQUE_INDEXES = {
  eventDedup: "sqlite_autoindex_events_1",
  indexerStateKey: "sqlite_autoindex_indexer_state_1",
  monitoredContractId: "sqlite_autoindex_monitored_contracts_1",
  webhookContractUrl: "sqlite_autoindex_webhook_subscriptions_1",
} as const;

/** Parameterized lookup SQL exercised by writer-pool write paths. */
export const WRITER_POOL_QUERIES = {
  eventDedup:
    "SELECT id FROM events WHERE contract_id = ? AND ledger_sequence = ? AND event_type = ?",
  eventContractLedger:
    "SELECT id FROM events WHERE contract_id = ? AND ledger_sequence = ?",
  ledgerPointer:
    "SELECT value FROM indexer_state WHERE key = ?",
  updateLedger:
    "UPDATE indexer_state SET value = ? WHERE key = ?",
  contractById:
    "SELECT * FROM monitored_contracts WHERE contract_id = ?",
  updateContract:
    "UPDATE monitored_contracts SET active = 0 WHERE contract_id = ?",
  activeContracts:
    "SELECT contract_id FROM monitored_contracts WHERE active = 1",
  webhookByContract:
    "SELECT * FROM webhook_subscriptions WHERE contract_id = ?",
  webhookByContractUrl:
    "SELECT * FROM webhook_subscriptions WHERE contract_id = ? AND webhook_url = ?",
  webhookByUrl:
    "SELECT * FROM webhook_subscriptions WHERE webhook_url = ?",
  deleteWebhookByUrl:
    "DELETE FROM webhook_subscriptions WHERE webhook_url = ?",
  schemaVersionLookup:
    "SELECT version FROM schema_migrations WHERE version = ?",
} as const;

export interface WriterPoolIndexReport {
  valid: boolean;
  present: string[];
  missing: string[];
}

function listIndexNames(database: Database.Database): string[] {
  return (
    database
      .prepare("SELECT name FROM sqlite_master WHERE type = 'index'")
      .all() as Array<{ name: string }>
  ).map((row) => row.name);
}

/**
 * Confirm every named and uniqueness index the writer pool relies on exists.
 */
export function verifyWriterPoolIndexes(
  targetDb?: Database.Database,
): WriterPoolIndexReport {
  const database = targetDb ?? getDb();
  const names = new Set(listIndexNames(database));
  const expected = [
    ...Object.values(WRITER_POOL_INDEXES),
    ...Object.values(WRITER_POOL_UNIQUE_INDEXES),
  ];
  const present = expected.filter((name) => names.has(name));
  const missing = expected.filter((name) => !names.has(name));
  return { valid: missing.length === 0, present, missing };
}

/**
 * Return SQLite EXPLAIN QUERY PLAN rows for a writer-pool lookup.
 */
export function explainWriterPoolQueryPlan(
  sql: string,
  params: unknown[] = [],
  targetDb?: Database.Database,
): Array<Record<string, unknown>> {
  const database = targetDb ?? getDb();
  return database
    .prepare(`EXPLAIN QUERY PLAN ${sql}`)
    .all(...params) as Array<Record<string, unknown>>;
}

/** True when any EXPLAIN QUERY PLAN detail references `indexName`. */
export function writerPoolQueryPlanUsesIndex(
  plan: Array<Record<string, unknown>>,
  indexName: string,
): boolean {
  return plan.some((row) =>
    Object.values(row).some(
      (value) => typeof value === "string" && value.includes(indexName),
    ),
  );
}

/** True when the planner would build a temporary B-tree (sort / group). */
export function writerPoolQueryPlanUsesTempBTree(
  plan: Array<Record<string, unknown>>,
): boolean {
  return plan.some((row) =>
    Object.values(row).some(
      (value) =>
        typeof value === "string" &&
        /USE TEMP B-TREE/i.test(value),
    ),
  );
}

/**
 * Verify the database schema the pool writes through: the migrations table
 * exists, every shipped migration is applied, the expected tables, columns,
 * and write-path indexes are present, and any registered hooks pass.
 *
 * Returns a report instead of throwing so callers can log or degrade; use
 * `assertWriterPoolSchemaReady` to fail fast.
 */
export function verifyWriterPoolSchema(): WriterPoolSchemaReport {
  const issues: string[] = [];
  let appliedVersions: number[] = [];
  let missingVersions: number[] = [];

  try {
    verifySchemaUpToDate();
  } catch (err) {
    issues.push(err instanceof Error ? err.message : String(err));
  }

  try {
    const db = getDb();
    appliedVersions = (
      db
        .prepare("SELECT version FROM schema_migrations ORDER BY version")
        .all() as Array<{ version: number }>
    ).map((row) => row.version);
    const applied = new Set(appliedVersions);
    missingVersions = getShippedMigrationVersions().filter(
      (version) => !applied.has(version),
    );
  } catch {
    // verifySchemaUpToDate has already reported the missing table.
    missingVersions = getShippedMigrationVersions();
  }

  const integrity = verifySchemaIntegrity();
  if (!integrity.valid) {
    issues.push(
      ...integrity.missingTables.map((table) => `missing table: ${table}`),
      ...Object.entries(integrity.missingColumns).map(
        ([table, columns]) => `missing columns in ${table}: ${columns.join(", ")}`,
      ),
      ...integrity.errors,
    );
  }

  try {
    const indexReport = verifyWriterPoolIndexes();
    if (!indexReport.valid) {
      issues.push(
        ...indexReport.missing.map((name) => `missing index: ${name}`),
      );
    }
  } catch (err) {
    issues.push(
      `writer-pool indexes unreadable: ${err instanceof Error ? err.message : String(err)}`,
    );
  }

  for (const [name, hook] of migrationHooks) {
    try {
      const result = hook(getDb());
      const hookIssues =
        typeof result === "string" ? [result] : Array.isArray(result) ? result : [];
      issues.push(...hookIssues.map((issue) => `${name}: ${issue}`));
    } catch (err) {
      issues.push(
        `${name}: hook threw ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }

  return {
    valid: issues.length === 0,
    appliedVersions,
    missingVersions,
    issues,
  };
}

/**
 * Verify the schema and throw `WriterPoolSchemaError` when it is out of sync.
 */
export function assertWriterPoolSchemaReady(): WriterPoolSchemaReport {
  const report = verifyWriterPoolSchema();
  if (!report.valid) {
    throw new WriterPoolSchemaError(
      `database_writer_pool cannot start – database schema is out of sync: ${report.issues.join("; ")}`,
      report.issues,
    );
  }
  return report;
}

export interface WriterPoolStartOptions {
  /**
   * Reject queued writes until a start succeeds. Defaults to false so callers
   * that never call `startWriterPool` keep working exactly as before.
   */
  enforce?: boolean;
}

let poolStarted = false;
let enforceStart = false;
let lastSchemaReport: WriterPoolSchemaReport | null = null;

/**
 * Validate the schema and mark the pool ready for writes.
 *
 * Throws `WriterPoolSchemaError` when the database state is out of sync, so a
 * process started against a stale database fails immediately instead of
 * writing through a schema it does not understand.
 */
export function startWriterPool(
  options: WriterPoolStartOptions = {},
): WriterPoolSchemaReport {
  const startedAt = performance.now();

  logWriterPoolDiagnostics({
    pool: POOL_NAME,
    operation: "start_writer_pool",
    status: "started",
    elapsedMs: 0,
    queueDepth: writeQueue.length,
  });

  try {
    const report = assertWriterPoolSchemaReady();

    poolStarted = true;
    enforceStart = options.enforce ?? false;
    lastSchemaReport = report;

    logWriterPoolDiagnostics({
      pool: POOL_NAME,
      operation: "start_writer_pool",
      status: "success",
      elapsedMs: roundElapsed(performance.now() - startedAt),
      queueDepth: writeQueue.length,
    });
    logger.info("database_writer_pool started", {
      pool: POOL_NAME,
      appliedVersions: report.appliedVersions,
      enforce: enforceStart,
    });

    return report;
  } catch (err) {
    const error = err instanceof Error ? err.message : String(err);

    poolStarted = false;
    lastSchemaReport =
      err instanceof WriterPoolSchemaError
        ? { valid: false, appliedVersions: [], missingVersions: [], issues: err.issues }
        : null;

    logWriterPoolDiagnostics({
      pool: POOL_NAME,
      operation: "start_writer_pool",
      status: "failure",
      elapsedMs: roundElapsed(performance.now() - startedAt),
      error,
    });
    logger.error("database_writer_pool failed to start", {
      pool: POOL_NAME,
      error,
    });

    throw err;
  }
}

/** Stop the pool. Queued writes are rejected again when `enforce` was set. */
export function stopWriterPool(): void {
  poolStarted = false;
  logger.info("database_writer_pool stopped", { pool: POOL_NAME });
}

export function isWriterPoolStarted(): boolean {
  return poolStarted;
}

/** The report from the most recent start attempt, or null if never started. */
export function getWriterPoolSchemaReport(): WriterPoolSchemaReport | null {
  return lastSchemaReport;
}

/** Reset start/enforcement state and registered hooks. Intended for tests. */
export function resetWriterPoolStartState(): void {
  poolStarted = false;
  enforceStart = false;
  lastSchemaReport = null;
  migrationHooks.clear();
}
