import {
  getDb,
  insertEvent,
  getShippedMigrationVersions,
  verifySchemaIntegrity,
  verifySchemaUpToDate,
  type EventRow,
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
 * - In-memory queue locks for concurrent event notifications (#327)
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
// In-memory event queue locks (#327)
// ---------------------------------------------------------------------------

/** Default ceiling on event rows held in memory before an overflow is raised. */
export const DEFAULT_WRITER_POOL_EVENT_QUEUE_MAX_SIZE = 10_000;

export class WriterPoolEventQueueOverflowError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "WriterPoolEventQueueOverflowError";
  }
}

/**
 * Persist a single event row. Returns true when a new row was written and
 * false when the store already held it. Defaults to a `queueWrite` +
 * `INSERT OR IGNORE` so inserts still go through the writer pool.
 */
export type WriterPoolEventPersistFn = (
  event: EventRow,
) => boolean | Promise<boolean>;

export interface WriterPoolEventQueueOptions {
  persist?: WriterPoolEventPersistFn;
  maxQueueSize?: number;
  /** Instance name used in queue diagnostics. */
  name?: string;
}

export interface WriterPoolEventEnqueueResult {
  queuedCount: number;
  duplicateCount: number;
}

export interface WriterPoolEventFlushResult {
  processedCount: number;
  insertedCount: number;
  duplicateCount: number;
}

export interface WriterPoolEventSubmitResult {
  queuedCount: number;
  insertedCount: number;
  duplicateCount: number;
}

/** Identity used by the events table UNIQUE(contract_id, ledger_sequence, event_type). */
export function writerPoolEventIdentityKey(
  event: Pick<EventRow, "contractId" | "eventType" | "ledgerSequence">,
): string {
  return `${event.contractId}|${event.ledgerSequence}|${event.eventType}`;
}

function validatePositiveInt(name: string, value: unknown): number {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 1) {
    throw new Error(`${name} must be a positive integer, received ${String(value)}`);
  }
  return value;
}

/**
 * Persist through the writer pool so concurrent notifications share the same
 * transaction + retry path as every other `queueWrite` caller.
 */
async function defaultPersistEvent(event: EventRow): Promise<boolean> {
  const result = await queueWrite({
    name: "insert-event",
    execute: () =>
      insertEvent(
        event.contractId,
        event.eventType,
        event.ledgerSequence,
        event.timestamp,
        event.dataJson,
      ),
  });
  if (!result.success) {
    throw result.error ?? new Error("insert-event failed");
  }
  return Boolean(result.data);
}

/**
 * Bounded in-memory queue that serializes event inserts per event identity.
 *
 * Concurrent `database_writer_pool` notifications routinely carry the same
 * event (overlapping poll windows, retried pages, several writers in one
 * process). Without a lock, two callers can both observe "not indexed yet"
 * and both insert. The queue closes that window: every row is drained under
 * a lock keyed on `contractId|ledgerSequence|eventType`, and the persisted-
 * key set is checked inside that lock, so exactly one caller writes each
 * event. Unrelated events still persist concurrently.
 */
export class WriterPoolEventQueue {
  readonly name: string;
  readonly maxQueueSize: number;

  private readonly persist: WriterPoolEventPersistFn;
  private readonly pending: EventRow[] = [];
  private readonly pendingKeys = new Set<string>();
  private readonly persistedKeys = new Set<string>();
  private readonly lockTails = new Map<string, Promise<void>>();
  private readonly heldLocks = new Set<string>();
  private queueMutex: Promise<void> = Promise.resolve();

  constructor(options: WriterPoolEventQueueOptions = {}) {
    this.name = options.name ?? "database_writer_pool";
    this.persist = options.persist ?? defaultPersistEvent;
    this.maxQueueSize = validatePositiveInt(
      "maxQueueSize",
      options.maxQueueSize ?? DEFAULT_WRITER_POOL_EVENT_QUEUE_MAX_SIZE,
    );
  }

  /** Rows currently waiting to be flushed. */
  get size(): number {
    return this.pending.length;
  }

  /** Event locks held right now – exposed for concurrency assertions. */
  get heldLockCount(): number {
    return this.heldLocks.size;
  }

  /** Distinct event identities this queue has already persisted. */
  get persistedKeyCount(): number {
    return this.persistedKeys.size;
  }

  hasPersisted(
    event: Pick<EventRow, "contractId" | "eventType" | "ledgerSequence">,
  ): boolean {
    return this.persistedKeys.has(writerPoolEventIdentityKey(event));
  }

  /** Drop all queue state. Intended for tests and process restarts. */
  reset(): void {
    this.pending.length = 0;
    this.pendingKeys.clear();
    this.persistedKeys.clear();
    this.lockTails.clear();
    this.heldLocks.clear();
    this.queueMutex = Promise.resolve();
  }

  /**
   * Serialize mutations of the queue structure itself, so concurrent
   * enqueue/flush callers never interleave a read and a write of `pending`.
   */
  private async withQueueMutex<T>(fn: () => T): Promise<T> {
    const previous = this.queueMutex;
    let release!: () => void;
    this.queueMutex = new Promise<void>((resolve) => {
      release = resolve;
    });
    try {
      await previous;
      return fn();
    } finally {
      release();
    }
  }

  /**
   * Serialize work for one event identity. Unrelated keys run concurrently and
   * the lock is always released, including when `fn` throws.
   */
  private async withEventLock<T>(
    key: string,
    fn: () => Promise<T> | T,
  ): Promise<T> {
    const previous = this.lockTails.get(key) ?? Promise.resolve();
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const tail = previous.then(
      () => gate,
      () => gate,
    );
    this.lockTails.set(key, tail);

    try {
      await previous.catch(() => undefined);
      this.heldLocks.add(key);
      return await fn();
    } finally {
      this.heldLocks.delete(key);
      release();
      if (this.lockTails.get(key) === tail) {
        this.lockTails.delete(key);
      }
    }
  }

  /**
   * Queue rows for insertion, dropping any already queued or already
   * persisted. Throws `WriterPoolEventQueueOverflowError` past `maxQueueSize`.
   */
  async enqueue(events: EventRow[]): Promise<WriterPoolEventEnqueueResult> {
    return this.withQueueMutex(() => {
      let queuedCount = 0;
      let duplicateCount = 0;

      for (const event of events) {
        const key = writerPoolEventIdentityKey(event);
        if (this.pendingKeys.has(key) || this.persistedKeys.has(key)) {
          duplicateCount++;
          continue;
        }
        if (this.pending.length >= this.maxQueueSize) {
          throw new WriterPoolEventQueueOverflowError(
            `${this.name} event queue is full (maxQueueSize=${this.maxQueueSize})`,
          );
        }
        this.pendingKeys.add(key);
        this.pending.push(event);
        queuedCount++;
      }

      return { queuedCount, duplicateCount };
    });
  }

  /** Drain the queue, persisting each row under its own event lock. */
  async flush(): Promise<WriterPoolEventFlushResult> {
    let processedCount = 0;
    let insertedCount = 0;
    let duplicateCount = 0;

    for (;;) {
      const next = await this.withQueueMutex(() => this.pending.shift());
      if (!next) break;

      const key = writerPoolEventIdentityKey(next);
      processedCount++;

      await this.withEventLock(key, async () => {
        try {
          if (this.persistedKeys.has(key)) {
            duplicateCount++;
            return;
          }
          const inserted = await this.persist(next);
          this.persistedKeys.add(key);
          if (inserted) {
            insertedCount++;
          } else {
            duplicateCount++;
          }
        } finally {
          this.pendingKeys.delete(key);
        }
      });
    }

    return { processedCount, insertedCount, duplicateCount };
  }

  /** Enqueue and flush in one step – the entry point for event notifications. */
  async submit(events: EventRow[]): Promise<WriterPoolEventSubmitResult> {
    const enqueued = await this.enqueue(events);
    const flushed = await this.flush();

    const result: WriterPoolEventSubmitResult = {
      queuedCount: enqueued.queuedCount,
      insertedCount: flushed.insertedCount,
      duplicateCount: enqueued.duplicateCount + flushed.duplicateCount,
    };

    logger.debug("database_writer_pool event queue submit", {
      queue: this.name,
      submitted: events.length,
      ...result,
    });

    return result;
  }
}

const defaultEventQueue = new WriterPoolEventQueue();

/** The process-wide queue used by `submitEventNotifications`. */
export function getWriterPoolEventQueue(): WriterPoolEventQueue {
  return defaultEventQueue;
}

/**
 * Index a batch of event notifications through the locked memory queue and
 * the writer pool. Concurrent callers sharing an event identity collapse to
 * a single insert; unrelated identities proceed in parallel.
 */
export function submitEventNotifications(
  events: EventRow[],
): Promise<WriterPoolEventSubmitResult> {
  return defaultEventQueue.submit(events);
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

/** Reset start/enforcement state, registered hooks, and event-queue locks. Intended for tests. */
export function resetWriterPoolStartState(): void {
  poolStarted = false;
  enforceStart = false;
  lastSchemaReport = null;
  migrationHooks.clear();
  defaultEventQueue.reset();
}
