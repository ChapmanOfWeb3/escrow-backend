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
// Dynamic poller throttling parameters (#341)
// ---------------------------------------------------------------------------
//
// The collector sizes how often it polls for new metrics based on ledger
// processing load. When the network is idle the `totalEvents` snapshot stops
// growing, so the collection poll interval backs off toward the maximum; once
// events start flowing again the interval is pulled back to the minimum so
// telemetry stays fresh.

export interface IndexerMetricsThrottleParameters {
  /** Starting poll interval in ms before any load is observed. */
  baseIntervalMs: number;
  /** Floor for the collection poll interval in ms. */
  minIntervalMs: number;
  /** Ceiling for the collection poll interval in ms after long idle periods. */
  maxIntervalMs: number;
  /** Factor applied to the interval on each idle backing-off step. */
  idleMultiplier: number;
  /** Consecutive idle collections required before backing off. */
  idleThresholdCycles: number;
}

export interface IndexerMetricsThrottleState {
  /** Current effective collection poll interval in ms. */
  currentIntervalMs: number;
  /** Number of events processed during the last observed interval. */
  lastProcessedEventCount: number;
  /** Consecutive idle (zero new event) collections so far. */
  idleCycles: number;
  /** Timestamp of the most recent throttle adjustment. */
  lastAdjustmentAt: number;
}

const METRICS_BASE_POLL_INTERVAL_MS = parseInt(
  process.env.INDEXER_METRICS_POLL_INTERVAL_MS || "60000",
  10,
);
const METRICS_MIN_POLL_INTERVAL_MS = parseInt(
  process.env.INDEXER_METRICS_MIN_POLL_INTERVAL_MS || "15000",
  10,
);
const METRICS_MAX_POLL_INTERVAL_MS = parseInt(
  process.env.INDEXER_METRICS_MAX_POLL_INTERVAL_MS || "600000",
  10,
);
const METRICS_IDLE_MULTIPLIER = parseFloat(
  process.env.INDEXER_METRICS_IDLE_MULTIPLIER || "2",
);
const METRICS_IDLE_THRESHOLD_CYCLES = parseInt(
  process.env.INDEXER_METRICS_IDLE_THRESHOLD_CYCLES || "3",
  10,
);

let metricsThrottleState: IndexerMetricsThrottleState = {
  currentIntervalMs: METRICS_BASE_POLL_INTERVAL_MS,
  lastProcessedEventCount: 0,
  idleCycles: 0,
  lastAdjustmentAt: Date.now(),
};

/** Snapshot of the configured collector throttle parameters (read-only). */
export function getIndexerMetricsThrottleParameters(): IndexerMetricsThrottleParameters {
  return {
    baseIntervalMs: METRICS_BASE_POLL_INTERVAL_MS,
    minIntervalMs: METRICS_MIN_POLL_INTERVAL_MS,
    maxIntervalMs: METRICS_MAX_POLL_INTERVAL_MS,
    idleMultiplier: METRICS_IDLE_MULTIPLIER,
    idleThresholdCycles: METRICS_IDLE_THRESHOLD_CYCLES,
  };
}

/** Snapshot of the current collector throttle state (read-only copy). */
export function getIndexerMetricsThrottleState(): IndexerMetricsThrottleState {
  return { ...metricsThrottleState };
}

/** Reset the collector throttle state to defaults (useful for tests). */
export function resetIndexerMetricsThrottleState(): void {
  metricsThrottleState = {
    currentIntervalMs: METRICS_BASE_POLL_INTERVAL_MS,
    lastProcessedEventCount: 0,
    idleCycles: 0,
    lastAdjustmentAt: Date.now(),
  };
  lastCollectionSnapshot = null;
}

/** Poll interval the collector should wait before the next collection. */
export function getIndexerMetricsPollDelayMs(): number {
  return metricsThrottleState.currentIntervalMs;
}

/**
 * Adjust the collection poll interval based on ledger processing load (#341).
 *
 * A collection that observed zero new events means the network is idle: once
 * `idleThresholdCycles` consecutive idle collections have been seen, the poll
 * interval backs off (multiplied by `idleMultiplier`, capped at
 * `maxIntervalMs`). A collection that observed new events resets the interval
 * to `minIntervalMs` so telemetry stays responsive under load.
 *
 * @param processedEventCount - Number of new events observed since the last
 *   collection.
 * @returns Updated throttle state snapshot.
 */
export function adjustIndexerMetricsPollingInterval(
  processedEventCount: number,
): IndexerMetricsThrottleState {
  const state = metricsThrottleState;
  state.lastProcessedEventCount = processedEventCount;

  if (processedEventCount === 0) {
    // Idle network → the collection poll wait increases once enough
    // consecutive idle collections have been observed.
    state.idleCycles += 1;
    if (state.idleCycles >= METRICS_IDLE_THRESHOLD_CYCLES) {
      state.currentIntervalMs = Math.min(
        state.currentIntervalMs * METRICS_IDLE_MULTIPLIER,
        METRICS_MAX_POLL_INTERVAL_MS,
      );
    }
  } else {
    // Active network → pull the poll interval back to the minimum.
    state.idleCycles = 0;
    state.currentIntervalMs = METRICS_MIN_POLL_INTERVAL_MS;
  }

  state.lastAdjustmentAt = Date.now();

  logger.debug("indexer_metrics_collector throttle adjustment", {
    collector: COLLECTOR_NAME,
    processedEventCount,
    currentIntervalMs: state.currentIntervalMs,
    idleCycles: state.idleCycles,
  });

  return { ...state };
}

/**
 * Number of events processed between two metrics snapshots. When there is no
 * previous snapshot, falls back to the current `totalEvents` (anything indexed
 * so far counts as load). When the counts are equal the network is considered
 * idle (0 new events).
 */
export function computeIndexerMetricsProcessedCount(
  current: Pick<IndexerMetrics, "totalEvents" | "lastIndexedLedger">,
  previous?: Pick<IndexerMetrics, "totalEvents" | "lastIndexedLedger"> | null,
): number {
  if (!previous) {
    return current.totalEvents > 0 ? current.totalEvents : 0;
  }
  return Math.max(0, current.totalEvents - previous.totalEvents);
}

let lastCollectionSnapshot: Pick<
  IndexerMetrics,
  "totalEvents" | "lastIndexedLedger"
> | null = null;

/**
 * Record a completed metrics collection and update the collector's dynamic
 * poll interval based on the ledger processing load it observed (#341).
 *
 * The load is the delta in `totalEvents` between this snapshot and the previous
 * one. If the snapshot is unchanged (idle network) the poll interval backs off;
 * if new events appeared it is reset to the minimum.
 *
 * @param metrics - The most recently collected metrics snapshot.
 * @returns Updated throttle state snapshot.
 */
export function onIndexerMetricsCollected(
  metrics: IndexerMetrics,
): IndexerMetricsThrottleState {
  const processedEventCount = computeIndexerMetricsProcessedCount(
    metrics,
    lastCollectionSnapshot,
  );
  lastCollectionSnapshot = {
    totalEvents: metrics.totalEvents,
    lastIndexedLedger: metrics.lastIndexedLedger,
  };
  return adjustIndexerMetricsPollingInterval(processedEventCount);
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
