import { getDb } from "./db.js";
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
 * - Exponential backoff retry for RPC connection timeout errors
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

  try {
    while (writeQueue.length > 0) {
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
  let lastError: Error | null = null;
  let retryCount = 0;

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
    writeQueue.push({ operation, resolve, reject });
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
