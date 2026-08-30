import {
  getDb,
  getLastIndexedLedger,
  getShippedMigrationVersions,
  verifySchemaIntegrity,
  verifySchemaUpToDate,
  type EventRow,
} from "./db.js";
import {
  chunkLedgerRange,
  filterEventsToRange,
  LedgerRangeValidationError,
  resolveHistoricalLedgerRange,
  validateLedgerRange,
  type LedgerRange,
} from "./ledger-range-tracker.js";
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
 * - Dynamic historical start/end ledger ranges for custom event imports (#330)
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
  rpcRetries: number;
  executionTimeMs: number;
}

/**
 * Configuration for exponential backoff retry on RPC connection timeout errors.
 * Follows the same naming conventions as RpcRetryConfig in rpc-poller-client.
 */
export interface WriterPoolRpcRetryConfig {
  maxRetries: number;
  initialBackoffMs: number;
  backoffMultiplier: number;
  maxBackoffMs: number;
}

const DEFAULT_RPC_RETRY_CONFIG: WriterPoolRpcRetryConfig = {
  maxRetries: 5,
  initialBackoffMs: 1000,
  backoffMultiplier: 2,
  maxBackoffMs: 30_000,
};

let rpcRetryConfig: WriterPoolRpcRetryConfig = { ...DEFAULT_RPC_RETRY_CONFIG };

const RPC_RETRYABLE_PATTERNS = [
  "timeout",
  "ECONNRESET",
  "ECONNREFUSED",
  "ETIMEDOUT",
  "socket hang up",
  "network",
  "status 429",
  "status 503",
  "status 502",
  "request timeout",
  "connect timeout",
];

export function isRpcTimeoutError(err: unknown): boolean {
  if (!(err instanceof Error)) return false;
  const msg = err.message.toLowerCase();
  return RPC_RETRYABLE_PATTERNS.some((p) => msg.includes(p.toLowerCase()));
}

export function computeRpcBackoffMs(
  attempt: number,
  config: Pick<WriterPoolRpcRetryConfig, "initialBackoffMs" | "backoffMultiplier" | "maxBackoffMs">
): number {
  return Math.min(
    config.initialBackoffMs * Math.pow(config.backoffMultiplier, attempt),
    config.maxBackoffMs
  );
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function setWriterPoolRpcRetryConfig(config: Partial<WriterPoolRpcRetryConfig>): void {
  rpcRetryConfig = { ...rpcRetryConfig, ...config };
}

export function getWriterPoolRpcRetryConfig(): WriterPoolRpcRetryConfig {
  return { ...rpcRetryConfig };
}

export function resetWriterPoolRpcRetryConfig(): void {
  rpcRetryConfig = { ...DEFAULT_RPC_RETRY_CONFIG };
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
 * This handles DB-level retries; RPC-level retries are handled by the outer wrapper.
 *
 * @param operation The write operation to execute
 * @param maxRetries Maximum number of retry attempts for DB conflicts
 * @returns WriteResult with success status, data, error, and metrics (rpcRetries always 0)
 */
async function executeWriteWithDbRetry<T>(
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
        rpcRetries: 0,
        executionTimeMs,
      };
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      retryCount = attempt;

      const isRetryable =
        lastError.message.includes("database is locked") ||
        lastError.message.includes("SQLITE_BUSY");

      if (attempt < maxRetries && isRetryable) {
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
        rpcRetries: 0,
        executionTimeMs,
      };
    }
  }

  const executionTimeMs = Date.now() - startTime;
  return {
    success: false,
    error: lastError || new Error("Unknown write failure"),
    retries: retryCount,
    rpcRetries: 0,
    executionTimeMs,
  };
}

/**
 * Execute a write operation with two layers of retry:
 * 1. Outer: exponential backoff retry for RPC connection timeout errors (configurable)
 * 2. Inner: exponential backoff retry for SQLite database locked conflicts
 *
 * Only RPC timeout patterns are retried at the outer level. All other errors
 * (constraint violations, syntax errors, etc.) are propagated immediately
 * after the inner DB-retry layer completes.
 *
 * @param operation The write operation to execute
 * @param dbMaxRetries Maximum number of DB-conflict retries per RPC attempt
 * @returns WriteResult with success status, retries breakdown, and metrics
 */
async function executeWrite<T>(
  operation: WriteOperation<T>,
  dbMaxRetries: number = 3
): Promise<WriteResult<T>> {
  const operationName = operation.name || "unknown";
  let rpcRetryCount = 0;
  let lastResult: WriteResult<T> | null = null;

  for (let rpcAttempt = 0; rpcAttempt <= rpcRetryConfig.maxRetries; rpcAttempt++) {
    const result = await executeWriteWithDbRetry(operation, dbMaxRetries);
    lastResult = result;

    if (result.success) {
      if (rpcAttempt > 0) {
        logger.info("Write operation succeeded after RPC retry", {
          operationName,
          rpcRetries: rpcAttempt,
          executionTimeMs: result.executionTimeMs,
        });
      }
      return { ...result, rpcRetries: rpcAttempt };
    }

    if (
      rpcAttempt < rpcRetryConfig.maxRetries &&
      result.error &&
      isRpcTimeoutError(result.error)
    ) {
      const delay = computeRpcBackoffMs(rpcAttempt, rpcRetryConfig);
      logger.warn("Write operation failed with RPC timeout, retrying", {
        operationName,
        attempt: rpcAttempt + 1,
        maxRetries: rpcRetryConfig.maxRetries,
        backoffMs: delay,
        error: result.error.message,
      });

      await sleep(delay);
      rpcRetryCount = rpcAttempt + 1;
      continue;
    }

    return { ...result, rpcRetries: rpcAttempt };
  }

  return {
    ...(lastResult || {
      success: false,
      retries: 0,
      executionTimeMs: 0,
    }),
    rpcRetries: rpcRetryCount,
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

/**
 * Verify the database schema the pool writes through: the migrations table
 * exists, every shipped migration is applied, the expected tables and columns
 * are present, and any registered hooks pass.
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
  /** Inclusive historical import start ledger (#330). */
  startLedger?: number;
  /** Inclusive historical import end ledger (#330). */
  endLedger?: number;
  /** Ledgers per historical import page (default 100). */
  pageSize?: number;
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

    if (
      options.startLedger !== undefined ||
      options.endLedger !== undefined ||
      options.pageSize !== undefined
    ) {
      configureWriterPoolHistoricalRange({
        startLedger: options.startLedger,
        endLedger: options.endLedger,
        pageSize: options.pageSize,
      });
    }

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
      startLedger: historicalRangeConfig.startLedger,
      endLedger: historicalRangeConfig.endLedger,
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
  resetWriterPoolHistoricalRangeConfig();
}

// ---------------------------------------------------------------------------
// Dynamic historical sync ranges (#330)
// ---------------------------------------------------------------------------

/** Inclusive historical range page size; matches ledger_range_tracker. */
export const DEFAULT_WRITER_POOL_HISTORICAL_PAGE_SIZE = 100;

export interface WriterPoolHistoricalRangeConfig {
  /** Inclusive custom start ledger. Falls back to env / defaults. */
  startLedger?: number;
  /** Inclusive custom end ledger. Falls back to env / defaults. */
  endLedger?: number;
  /** Ledgers per historical import page. */
  pageSize?: number;
}

export interface WriterPoolHistoricalRangeOptions
  extends WriterPoolHistoricalRangeConfig {
  /** Fallback start when neither an explicit nor an env start is set. */
  defaultStart?: number;
  /** Fallback end when neither an explicit nor an env end is set. */
  defaultEnd?: number;
  /** Pre-fetched events; filtered to the resolved range before persist. */
  events?: EventRow[];
  /** Per-page event source. Called once per chunk with the page's inclusive range. */
  fetchEvents?: (page: LedgerRange) => Promise<EventRow[]> | EventRow[];
  /**
   * When true, advances `last_ledger_sequence` to the range end after a
   * successful import. Defaults to false so live polling is unchanged.
   */
  advanceLivePointer?: boolean;
}

/** Number of events indexed for a single ledger ("block"). */
export interface WriterPoolLedgerEventCount {
  ledgerSequence: number;
  eventCount: number;
}

export interface WriterPoolHistoricalImportResult {
  range: LedgerRange;
  pages: LedgerRange[];
  /** Events accepted into the requested range (pre-persist). */
  eventCount: number;
  /** Rows actually written by the pool. */
  insertedCount: number;
  /** Rows skipped as already present (INSERT OR IGNORE). */
  duplicateCount: number;
  /** Distinct ledgers ("blocks") that contributed at least one event. */
  processedLedgerCount: number;
  /** Per-ledger event counts, ascending by ledger sequence. */
  ledgerEventCounts: WriterPoolLedgerEventCount[];
  elapsedMs: number;
}

let historicalRangeConfig: WriterPoolHistoricalRangeConfig = {};

function defaultHistoricalStart(): number {
  const last = getLastIndexedLedger();
  return last < 1 ? 1 : last + 1;
}

function validateOptionalLedger(name: string, value: unknown): number {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 1) {
    throw new LedgerRangeValidationError(
      `${name} must be a positive integer, received ${String(value)}`
    );
  }
  return value;
}

/**
 * Aggregate per-ledger ("block") event counts, ascending by ledger sequence.
 * Tests assert against this to prove a custom range indexed every block.
 */
export function countWriterPoolEventsByLedger(
  events: Array<{ ledgerSequence?: unknown; ledger?: unknown }>
): WriterPoolLedgerEventCount[] {
  const counts = new Map<number, number>();
  for (const event of events) {
    const raw = event?.ledgerSequence ?? event?.ledger;
    const ledger = Number(raw);
    if (!Number.isFinite(ledger)) continue;
    counts.set(ledger, (counts.get(ledger) ?? 0) + 1);
  }
  return [...counts.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([ledgerSequence, eventCount]) => ({ ledgerSequence, eventCount }));
}

/**
 * Store optional historical start/end/pageSize on the pool.
 * When both start and end are supplied they are validated as a pair.
 */
export function configureWriterPoolHistoricalRange(
  options: WriterPoolHistoricalRangeConfig = {}
): WriterPoolHistoricalRangeConfig {
  if (options.startLedger !== undefined && options.endLedger !== undefined) {
    validateLedgerRange(options.startLedger, options.endLedger);
  } else {
    if (options.startLedger !== undefined) {
      validateOptionalLedger("start ledger", options.startLedger);
    }
    if (options.endLedger !== undefined) {
      validateOptionalLedger("end ledger", options.endLedger);
    }
  }
  if (options.pageSize !== undefined) {
    validateOptionalLedger("page size", options.pageSize);
  }
  historicalRangeConfig = { ...options };
  return { ...historicalRangeConfig };
}

export function getWriterPoolHistoricalRangeConfig(): WriterPoolHistoricalRangeConfig {
  return { ...historicalRangeConfig };
}

export function resetWriterPoolHistoricalRangeConfig(): void {
  historicalRangeConfig = {};
}

/**
 * Resolve an inclusive historical range from explicit values, then the
 * pool's configured start/end, then `LEDGER_RANGE_START` / `LEDGER_RANGE_END`,
 * then live defaults (`last_indexed + 1` → provided default end).
 *
 * Throws `LedgerRangeValidationError` on non-integers, values below 1, or
 * an inverted range (start > end).
 */
export function resolveWriterPoolHistoricalRange(
  options: WriterPoolHistoricalRangeOptions = {}
): LedgerRange {
  return resolveHistoricalLedgerRange({
    startLedger: options.startLedger ?? historicalRangeConfig.startLedger,
    endLedger: options.endLedger ?? historicalRangeConfig.endLedger,
    defaultStart: options.defaultStart ?? defaultHistoricalStart(),
    defaultEnd: options.defaultEnd ?? historicalRangeConfig.endLedger,
  });
}

/**
 * Import events for a custom inclusive historical ledger range through the
 * writer pool.
 *
 * The requested range is validated, split into pages, and used to filter
 * events before they are persisted. Historical imports never advance the
 * live ledger pointer unless `advanceLivePointer` is set, so live
 * synchronization is unaffected.
 */
export async function importHistoricalRange(
  options: WriterPoolHistoricalRangeOptions = {}
): Promise<WriterPoolHistoricalImportResult> {
  const startedAt = performance.now();
  const range = resolveWriterPoolHistoricalRange(options);
  const pageSize = validateOptionalLedger(
    "page size",
    options.pageSize ??
      historicalRangeConfig.pageSize ??
      DEFAULT_WRITER_POOL_HISTORICAL_PAGE_SIZE
  );
  const pages = chunkLedgerRange(range, pageSize);

  logWriterPoolDiagnostics({
    pool: POOL_NAME,
    operation: "import_historical_range",
    status: "started",
    elapsedMs: 0,
    queueDepth: writeQueue.length,
  });

  try {
    const collected: EventRow[] = [];

    if (options.fetchEvents) {
      for (const page of pages) {
        const pageEvents = await options.fetchEvents(page);
        collected.push(...filterEventsToRange(pageEvents, page));
      }
    } else if (options.events) {
      collected.push(...filterEventsToRange(options.events, range));
    }

    const persistResult = await executeWriteTransaction({
      name: "import_historical_range",
      execute: (db) => {
        const insertStmt = db.prepare(`
          INSERT OR IGNORE INTO events
          (contract_id, event_type, ledger_sequence, timestamp, data_json)
          VALUES (?, ?, ?, ?, ?)
        `);

        let insertedCount = 0;
        let duplicateCount = 0;

        for (const ev of collected) {
          const result = insertStmt.run(
            ev.contractId,
            ev.eventType,
            ev.ledgerSequence,
            ev.timestamp,
            ev.dataJson
          );
          if (result.changes > 0) {
            insertedCount += 1;
          } else {
            duplicateCount += 1;
          }
        }

        if (options.advanceLivePointer) {
          db.prepare(
            "UPDATE indexer_state SET value = ? WHERE key = 'last_ledger_sequence'"
          ).run(range.endLedger.toString());
        }

        return { insertedCount, duplicateCount };
      },
    });

    if (!persistResult.success) {
      throw persistResult.error ?? new Error("historical import write failed");
    }

    const elapsedMs = Math.max(0, performance.now() - startedAt);
    const ledgerEventCounts = countWriterPoolEventsByLedger(collected);
    const insertedCount = persistResult.data?.insertedCount ?? 0;
    const duplicateCount = persistResult.data?.duplicateCount ?? 0;

    logWriterPoolDiagnostics({
      pool: POOL_NAME,
      operation: "import_historical_range",
      status: "success",
      elapsedMs: roundElapsed(elapsedMs),
      payloadSizeBytes: writerPoolPayloadSizeBytes(collected),
      queueDepth: writeQueue.length,
    });
    logger.info("database_writer_pool historical range imported", {
      pool: POOL_NAME,
      startLedger: range.startLedger,
      endLedger: range.endLedger,
      eventCount: collected.length,
      insertedCount,
      duplicateCount,
      processedLedgerCount: ledgerEventCounts.length,
    });

    return {
      range,
      pages,
      eventCount: collected.length,
      insertedCount,
      duplicateCount,
      processedLedgerCount: ledgerEventCounts.length,
      ledgerEventCounts,
      elapsedMs,
    };
  } catch (err) {
    const error = err instanceof Error ? err.message : String(err);
    logWriterPoolDiagnostics({
      pool: POOL_NAME,
      operation: "import_historical_range",
      status: "failure",
      elapsedMs: roundElapsed(performance.now() - startedAt),
      queueDepth: writeQueue.length,
      error,
    });
    throw err;
  }
}
