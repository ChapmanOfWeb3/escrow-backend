import type Database from "better-sqlite3";
import { getDb } from "./db.js";
import logger from "../utils/logger.js";

/**
 * indexer_metrics_collector – poller performance telemetry tracker.
 *
 * Beyond collecting the metrics snapshot, this module carries:
 * - High-frequency polling diagnostics: every collection and every individual
 *   query emits a debug log whose message string carries `elapsedMs=` and
 *   `payloadSizeBytes=`, so operators can spot slow queries and oversized
 *   payloads without enabling a profiler (#337).
 * - Threshold alerting: consecutive collection failures and stalled
 *   collections raise warning alerts once the configured counts are reached
 *   (#338).
 */

export interface IndexerMetrics {
  lastIndexedLedger: number;
  totalEvents: number;
  lastEventAt: string | null;
  eventsByType: Record<string, number>;
  activeContractsCount: number;
  totalSubscriptions: number;
  collectedAt: string;
}

const COLLECTOR_NAME = "indexer_metrics_collector";

// ---------------------------------------------------------------------------
// Polling diagnostics (#337)
// ---------------------------------------------------------------------------

export interface IndexerMetricsDiagnostics {
  collector: string;
  operation: string;
  status: "started" | "success" | "failure" | "skipped";
  /** Wall-clock duration of the operation in milliseconds. */
  elapsedMs: number;
  payloadSizeBytes?: number;
  rowCount?: number;
  totalEvents?: number;
  lastIndexedLedger?: number;
  error?: string;
}

/** Byte size of a JSON-serialized payload, matching indexer_runner's helper. */
export function metricsPayloadSizeBytes(value: unknown): number {
  return Buffer.byteLength(JSON.stringify(value ?? null), "utf8");
}

/** Round to microsecond precision so sub-millisecond queries stay readable. */
function roundElapsed(elapsedMs: number): number {
  return Math.round(Math.max(0, elapsedMs) * 1000) / 1000;
}

/**
 * Emit an indexer_metrics_collector diagnostics debug log.
 *
 * The message string always carries `elapsedMs=` (and `payloadSizeBytes=` when
 * known) so log-scraping validation can assert timing values are present; the
 * same values are repeated in the structured meta object for log processors.
 */
export function logIndexerMetricsDiagnostics(
  diagnostics: IndexerMetricsDiagnostics,
): void {
  const parts = [
    `${diagnostics.collector} poll diagnostics`,
    `operation=${diagnostics.operation}`,
    `status=${diagnostics.status}`,
    `elapsedMs=${diagnostics.elapsedMs}`,
  ];
  if (diagnostics.payloadSizeBytes !== undefined) {
    parts.push(`payloadSizeBytes=${diagnostics.payloadSizeBytes}`);
  }
  if (diagnostics.rowCount !== undefined) {
    parts.push(`rowCount=${diagnostics.rowCount}`);
  }
  logger.debug(parts.join(" "), diagnostics);
}

/**
 * Time a single collection stage and emit its diagnostics. Failures are logged
 * with the elapsed time too, then rethrown for the caller to classify.
 */
function withStageDiagnostics<T>(operation: string, fn: () => T): T {
  const startedAt = performance.now();
  try {
    const result = fn();
    logIndexerMetricsDiagnostics({
      collector: COLLECTOR_NAME,
      operation,
      status: "success",
      elapsedMs: roundElapsed(performance.now() - startedAt),
      payloadSizeBytes: metricsPayloadSizeBytes(result),
      rowCount: Array.isArray(result) ? result.length : undefined,
    });
    return result;
  } catch (err) {
    logIndexerMetricsDiagnostics({
      collector: COLLECTOR_NAME,
      operation,
      status: "failure",
      elapsedMs: roundElapsed(performance.now() - startedAt),
      error: err instanceof Error ? err.message : String(err),
    });
    throw err;
  }
}

/**
 * Same as `withStageDiagnostics` for a query whose table may not exist yet:
 * a failure is reported as a skipped stage and yields `undefined` instead of
 * aborting the whole collection.
 */
function withOptionalStageDiagnostics<T>(
  operation: string,
  fn: () => T,
): T | undefined {
  const startedAt = performance.now();
  try {
    const result = fn();
    logIndexerMetricsDiagnostics({
      collector: COLLECTOR_NAME,
      operation,
      status: "success",
      elapsedMs: roundElapsed(performance.now() - startedAt),
      payloadSizeBytes: metricsPayloadSizeBytes(result),
      rowCount: Array.isArray(result) ? result.length : undefined,
    });
    return result;
  } catch (err) {
    logIndexerMetricsDiagnostics({
      collector: COLLECTOR_NAME,
      operation,
      status: "skipped",
      elapsedMs: roundElapsed(performance.now() - startedAt),
      error: err instanceof Error ? err.message : String(err),
    });
    return undefined;
  }
}

// ---------------------------------------------------------------------------
// Threshold alerting (#338)
// ---------------------------------------------------------------------------

/** Consecutive collection failures before a warning alert is raised. */
export const DEFAULT_METRICS_FAILURE_THRESHOLD = 3;

/** Elapsed ms without a successful collection before a stall alert is raised. */
export const DEFAULT_METRICS_STALL_THRESHOLD_MS = 120_000;

export type IndexerMetricsFailureType = "collection" | "query" | "stall";

function readPositiveIntEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 1) {
    logger.warn("indexer_metrics_collector ignoring invalid threshold config", {
      collector: COLLECTOR_NAME,
      variable: name,
      received: raw,
      fallback,
    });
    return fallback;
  }
  return value;
}

/**
 * Read alert thresholds from `INDEXER_METRICS_FAILURE_THRESHOLD` and
 * `INDEXER_METRICS_STALL_THRESHOLD_MS`. Invalid values fall back to the
 * defaults with a warning rather than throwing – telemetry must not take the
 * indexer down.
 */
export function getIndexerMetricsAlertConfig(): {
  failureThreshold: number;
  stallThresholdMs: number;
} {
  return {
    failureThreshold: readPositiveIntEnv(
      "INDEXER_METRICS_FAILURE_THRESHOLD",
      DEFAULT_METRICS_FAILURE_THRESHOLD,
    ),
    stallThresholdMs: readPositiveIntEnv(
      "INDEXER_METRICS_STALL_THRESHOLD_MS",
      DEFAULT_METRICS_STALL_THRESHOLD_MS,
    ),
  };
}

export interface IndexerMetricsMonitorOptions {
  name?: string;
  failureThreshold?: number;
  stallThresholdMs?: number;
}

/**
 * Tracks consecutive collection failures and collection stalls, raising a
 * warning alert once the configured counts are reached (#338).
 *
 * Every failure is logged as an error; the warning alert fires from the
 * threshold onwards so a persistent outage keeps surfacing rather than
 * alerting once and going quiet. A successful collection clears the state.
 */
export class IndexerMetricsFailureMonitor {
  readonly collector: string;
  readonly failureThreshold: number;
  readonly stallThresholdMs: number;

  private consecutiveFailures = 0;
  private lastSuccessfulAt: number | null = null;
  private alertActive = false;

  constructor(options: IndexerMetricsMonitorOptions = {}) {
    const env = getIndexerMetricsAlertConfig();
    this.collector = options.name ?? COLLECTOR_NAME;
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
   * Record a failed collection. Returns the new consecutive-failure count.
   * Emits an error every time and a warning alert from the threshold onwards.
   */
  recordFailure(
    failureType: IndexerMetricsFailureType,
    details: { error?: string; operation?: string } = {},
  ): number {
    this.consecutiveFailures += 1;

    const payload = {
      collector: this.collector,
      failureType,
      operation: details.operation,
      consecutiveFailures: this.consecutiveFailures,
      threshold: this.failureThreshold,
      error: details.error,
    };

    logger.error("indexer_metrics_collector operation failed", payload);

    if (this.consecutiveFailures >= this.failureThreshold) {
      this.alertActive = true;
      logger.warn(
        "indexer_metrics_collector alert: consecutive failure threshold reached",
        {
          ...payload,
          action:
            "Inspect the indexer database and metrics queries; alerting clears automatically after the next successful collection.",
        },
      );
    }

    return this.consecutiveFailures;
  }

  /** Record a successful collection, clearing any active alert. */
  recordSuccess(): void {
    const hadFailures = this.consecutiveFailures > 0 || this.alertActive;
    this.consecutiveFailures = 0;
    this.lastSuccessfulAt = Date.now();
    if (hadFailures) {
      logger.info("indexer_metrics_collector recovered after failures", {
        collector: this.collector,
      });
    }
    this.alertActive = false;
  }

  /**
   * Warn when no successful collection has landed inside the stall window.
   * Does not touch the consecutive-failure counter, so a later success still
   * recovers cleanly. Returns true when a stall was reported.
   */
  checkStall(): boolean {
    if (this.lastSuccessfulAt === null) return false;
    const elapsedMs = Date.now() - this.lastSuccessfulAt;
    if (elapsedMs <= this.stallThresholdMs) return false;

    logger.warn(
      "indexer_metrics_collector alert: collection stall threshold reached",
      {
        collector: this.collector,
        failureType: "stall" as const,
        consecutiveFailures: this.consecutiveFailures,
        threshold: this.failureThreshold,
        stallThresholdMs: this.stallThresholdMs,
        elapsedMs,
        action:
          "No successful metrics collection within the stall window; inspect the poller and database health.",
      },
    );
    return true;
  }

  reset(): void {
    this.consecutiveFailures = 0;
    this.lastSuccessfulAt = null;
    this.alertActive = false;
  }
}

let defaultMonitor = new IndexerMetricsFailureMonitor();

/** The monitor backing `collectIndexerMetrics`. */
export function getIndexerMetricsMonitor(): IndexerMetricsFailureMonitor {
  return defaultMonitor;
}

/**
 * Clear collector alert state and re-read the threshold configuration.
 * Intended for tests and for reloads after a config change.
 */
export function resetIndexerMetricsCollectorState(): void {
  defaultMonitor = new IndexerMetricsFailureMonitor();
}

// ---------------------------------------------------------------------------
// Collection
// ---------------------------------------------------------------------------

/**
 * Collect indexer metrics using transaction isolation to ensure
 * a consistent snapshot across all metrics queries.
 *
 * Each query and the collection as a whole emit debug diagnostics carrying
 * elapsed time and payload size (#337). Failures increment the consecutive
 * failure counter and raise a threshold alert (#338) before rethrowing, so
 * callers keep their existing error handling.
 */
export function collectIndexerMetrics(
  targetDb?: Database.Database,
): IndexerMetrics {
  const database = targetDb || getDb();
  const monitor = defaultMonitor;
  const startedAt = performance.now();

  monitor.checkStall();

  logIndexerMetricsDiagnostics({
    collector: COLLECTOR_NAME,
    operation: "collect_metrics",
    status: "started",
    elapsedMs: 0,
  });

  // Execute all metric queries within a database transaction for isolation
  const getMetricsTx = database.transaction(() => {
    const lastLedgerRow = withStageDiagnostics("query_last_ledger", () =>
      database
        .prepare(
          "SELECT value FROM indexer_state WHERE key = 'last_ledger_sequence'",
        )
        .get() as { value: string } | undefined,
    );
    const lastIndexedLedger = lastLedgerRow ? parseInt(lastLedgerRow.value, 10) : 0;

    const totalRow = withStageDiagnostics(
      "query_total_events",
      () =>
        database.prepare("SELECT COUNT(*) as count FROM events").get() as {
          count: number;
        },
    );

    const lastEventRow = withStageDiagnostics(
      "query_last_event_at",
      () =>
        database
          .prepare("SELECT MAX(created_at) as last_at FROM events")
          .get() as { last_at: string | null },
    );

    const typeRows = withStageDiagnostics(
      "query_events_by_type",
      () =>
        database
          .prepare(
            "SELECT event_type, COUNT(*) as count FROM events GROUP BY event_type",
          )
          .all() as Array<{ event_type: string; count: number }>,
    );

    const eventsByType: Record<string, number> = {};
    for (const row of typeRows) {
      eventsByType[row.event_type] = row.count;
    }

    // monitored_contracts / webhook_subscriptions might not exist yet, so a
    // failure here is reported as a skipped stage rather than a collection
    // failure – matching the collector's original behaviour.
    const activeContractsRow = withOptionalStageDiagnostics(
      "query_active_contracts",
      () =>
        database
          .prepare(
            "SELECT COUNT(*) as count FROM monitored_contracts WHERE active = 1",
          )
          .get() as { count: number } | undefined,
    );
    const activeContractsCount = activeContractsRow
      ? activeContractsRow.count
      : 0;

    const subscriptionsRow = withOptionalStageDiagnostics(
      "query_subscriptions",
      () =>
        database
          .prepare("SELECT COUNT(*) as count FROM webhook_subscriptions")
          .get() as { count: number } | undefined,
    );
    const totalSubscriptions = subscriptionsRow ? subscriptionsRow.count : 0;

    return {
      lastIndexedLedger,
      totalEvents: totalRow ? totalRow.count : 0,
      lastEventAt: lastEventRow ? lastEventRow.last_at : null,
      eventsByType,
      activeContractsCount,
      totalSubscriptions,
      collectedAt: new Date().toISOString(),
    };
  });

  try {
    const metrics = getMetricsTx();

    monitor.recordSuccess();
    logIndexerMetricsDiagnostics({
      collector: COLLECTOR_NAME,
      operation: "collect_metrics",
      status: "success",
      elapsedMs: roundElapsed(performance.now() - startedAt),
      payloadSizeBytes: metricsPayloadSizeBytes(metrics),
      totalEvents: metrics.totalEvents,
      lastIndexedLedger: metrics.lastIndexedLedger,
    });

    return metrics;
  } catch (err) {
    const error = err instanceof Error ? err.message : String(err);

    logIndexerMetricsDiagnostics({
      collector: COLLECTOR_NAME,
      operation: "collect_metrics",
      status: "failure",
      elapsedMs: roundElapsed(performance.now() - startedAt),
      error,
    });
    monitor.recordFailure("collection", {
      error,
      operation: "collect_metrics",
    });

    throw err;
  }
}

// ---------------------------------------------------------------------------
// Dynamic historical sync ranges
// ---------------------------------------------------------------------------

/** Per-ledger ("block") event count, ascending by ledger sequence. */
export interface IndexerHistoricalLedgerEventCount {
  ledgerSequence: number;
  eventCount: number;
}

export interface HistoricalMetricsRangeOptions {
  /** Inclusive lower bound of the ledger range to import. */
  startLedger: number;
  /** Inclusive upper bound of the ledger range to import. */
  endLedger: number;
}

export interface HistoricalMetricsResult {
  range: { startLedger: number; endLedger: number };
  /** Total events indexed within the requested range. */
  totalEvents: number;
  /** Event counts grouped by type, within the range. */
  eventsByType: Record<string, number>;
  /** Last indexed ledger in the overall database (not range-limited). */
  lastIndexedLedger: number;
  collectedAt: string;
  /** Per-ledger ("block") event counts, ascending by ledger sequence. */
  ledgerEventCounts: IndexerHistoricalLedgerEventCount[];
  /** Number of distinct ledgers in the range that have at least one event. */
  processedLedgerCount: number;
}

export interface HistoricalRangeValidation {
  ok: boolean;
  error?: string;
}

/**
 * Validate an inclusive ledger range for historical metrics collection.
 * Both values must be positive integers and startLedger must be ≤ endLedger.
 */
export function validateHistoricalRange(
  startLedger: number,
  endLedger: number,
): HistoricalRangeValidation {
  if (
    typeof startLedger !== "number" ||
    !Number.isInteger(startLedger) ||
    startLedger < 1
  ) {
    return {
      ok: false,
      error: `startLedger must be a positive integer, got: ${startLedger}`,
    };
  }
  if (
    typeof endLedger !== "number" ||
    !Number.isInteger(endLedger) ||
    endLedger < 1
  ) {
    return {
      ok: false,
      error: `endLedger must be a positive integer, got: ${endLedger}`,
    };
  }
  if (startLedger > endLedger) {
    return {
      ok: false,
      error: `startLedger (${startLedger}) must be ≤ endLedger (${endLedger})`,
    };
  }
  return { ok: true };
}

/**
 * Collect metrics for a custom historical ledger range, accepting dynamic
 * start/end ledger values for custom historical event imports.
 *
 * The function validates the range, then queries the `events` table inside a
 * transaction for a consistent snapshot. It returns per-ledger ("block") event
 * counts so callers can assert the correct number of events were indexed for
 * each block in the imported range.
 *
 * Unlike `collectIndexerMetrics` this never writes to the database and does not
 * advance the live `last_ledger_sequence` pointer — it is purely a read-side
 * verification of an already-completed historical import.
 */
export function collectHistoricalMetrics(
  options: HistoricalMetricsRangeOptions,
  targetDb?: Database.Database,
): HistoricalMetricsResult {
  const { startLedger, endLedger } = options;

  const validation = validateHistoricalRange(startLedger, endLedger);
  if (!validation.ok) {
    throw new Error(validation.error);
  }

  const database = targetDb || getDb();
  const monitor = defaultMonitor;
  const startedAt = performance.now();

  logIndexerMetricsDiagnostics({
    collector: COLLECTOR_NAME,
    operation: "collect_historical_metrics",
    status: "started",
    elapsedMs: 0,
    startLedger,
    endLedger,
  });

  const getMetricsTx = database.transaction(() => {
    const lastLedgerRow = withStageDiagnostics(
      "query_historical_last_ledger",
      () =>
        database
          .prepare(
            "SELECT value FROM indexer_state WHERE key = 'last_ledger_sequence'",
          )
          .get() as { value: string } | undefined,
    );
    const lastIndexedLedger = lastLedgerRow
      ? parseInt(lastLedgerRow.value, 10)
      : 0;

    const totalRow = withStageDiagnostics(
      "query_historical_total_events",
      () =>
        database
          .prepare(
            `SELECT COUNT(*) as count FROM events
             WHERE ledger_sequence >= ? AND ledger_sequence <= ?`,
          )
          .get(startLedger, endLedger) as { count: number },
    );

    const typeRows = withStageDiagnostics(
      "query_historical_events_by_type",
      () =>
        database
          .prepare(
            `SELECT event_type, COUNT(*) as count FROM events
             WHERE ledger_sequence >= ? AND ledger_sequence <= ?
             GROUP BY event_type`,
          )
          .all(startLedger, endLedger) as Array<{ event_type: string; count: number }>,
    );

    const eventsByType: Record<string, number> = {};
    for (const row of typeRows) {
      eventsByType[row.event_type] = row.count;
    }

    const ledgerRows = withStageDiagnostics(
      "query_historical_ledger_event_counts",
      () =>
        database
          .prepare(
            `SELECT ledger_sequence, COUNT(*) as count FROM events
             WHERE ledger_sequence >= ? AND ledger_sequence <= ?
             GROUP BY ledger_sequence
             ORDER BY ledger_sequence ASC`,
          )
          .all(startLedger, endLedger) as Array<{
            ledger_sequence: number;
            count: number;
          }>,
    );

    const ledgerEventCounts: IndexerHistoricalLedgerEventCount[] = ledgerRows.map(
      (row) => ({
        ledgerSequence: row.ledger_sequence,
        eventCount: row.count,
      }),
    );

    return {
      range: { startLedger, endLedger },
      totalEvents: totalRow ? totalRow.count : 0,
      eventsByType,
      lastIndexedLedger,
      collectedAt: new Date().toISOString(),
      ledgerEventCounts,
      processedLedgerCount: ledgerEventCounts.length,
    };
  });

  try {
    const metrics = getMetricsTx();

    monitor.recordSuccess();
    logIndexerMetricsDiagnostics({
      collector: COLLECTOR_NAME,
      operation: "collect_historical_metrics",
      status: "success",
      elapsedMs: roundElapsed(performance.now() - startedAt),
      payloadSizeBytes: metricsPayloadSizeBytes(metrics),
      startLedger,
      endLedger,
      totalEvents: metrics.totalEvents,
      lastIndexedLedger: metrics.lastIndexedLedger,
    });

    return metrics;
  } catch (err) {
    const error = err instanceof Error ? err.message : String(err);

    logIndexerMetricsDiagnostics({
      collector: COLLECTOR_NAME,
      operation: "collect_historical_metrics",
      status: "failure",
      elapsedMs: roundElapsed(performance.now() - startedAt),
      startLedger,
      endLedger,
      error,
    });
    monitor.recordFailure("collection", {
      error,
      operation: "collect_historical_metrics",
    });

    throw err;
  }
}
