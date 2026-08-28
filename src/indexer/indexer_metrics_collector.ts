import type Database from "better-sqlite3";
import { getDb, insertEvent, type EventRow } from "./db.js";
import logger from "../utils/logger.js";

/**
 * indexer_metrics_collector – poller performance telemetry tracker.
 *
 * Alongside the metrics snapshot this module carries:
 * - In-memory queue locks so concurrent event notifications are inserted once
 *   and concurrent collections share a single consistent snapshot (#336).
 * - The SQLite index structures the collector's queries depend on, plus
 *   EXPLAIN QUERY PLAN helpers to prove they are used (#335).
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
// SQLite index structures (#335)
// ---------------------------------------------------------------------------

/**
 * The exact statements the collector runs. Exported so query-plan checks
 * verify the real queries rather than a copy that can drift.
 */
export const INDEXER_METRICS_QUERIES = {
  lastLedger:
    "SELECT value FROM indexer_state WHERE key = 'last_ledger_sequence'",
  totalEvents: "SELECT COUNT(*) as count FROM events",
  lastEventAt: "SELECT MAX(created_at) as last_at FROM events",
  eventsByType:
    "SELECT event_type, COUNT(*) as count FROM events GROUP BY event_type",
  activeContracts:
    "SELECT COUNT(*) as count FROM monitored_contracts WHERE active = 1",
  subscriptions: "SELECT COUNT(*) as count FROM webhook_subscriptions",
} as const;

/**
 * Indexes the collector's lookups rely on.
 *
 * `idx_events_event_type` is added by migration 6 for the events-by-type
 * aggregation: without a leading `event_type` index SQLite falls back to
 * "USE TEMP B-TREE FOR GROUP BY" over the whole table. The other two already
 * ship with earlier migrations and are listed here so a regression in either
 * one is caught by the same verification.
 */
export const INDEXER_METRICS_INDEXES = {
  /** GROUP BY event_type aggregation. */
  eventsByType: "idx_events_event_type",
  /** MAX(created_at) lookup for the newest event. */
  lastEventAt: "idx_events_created_at",
  /** Active monitored-contract count. */
  activeContracts: "idx_monitored_contracts_active",
} as const;

/** Return the EXPLAIN QUERY PLAN rows for a statement. */
export function explainIndexerMetricsQueryPlan(
  sql: string,
  targetDb?: Database.Database,
): Array<Record<string, unknown>> {
  const database = targetDb || getDb();
  return database.prepare(`EXPLAIN QUERY PLAN ${sql}`).all() as Array<
    Record<string, unknown>
  >;
}

/** True when any plan row references the expected index name. */
export function metricsQueryPlanUsesIndex(
  plan: Array<Record<string, unknown>>,
  indexName: string,
): boolean {
  return plan.some((row) =>
    Object.values(row).some(
      (value) => typeof value === "string" && value.includes(indexName),
    ),
  );
}

/** True when the plan resorts to a temporary B-tree instead of an index. */
export function metricsQueryPlanUsesTempBTree(
  plan: Array<Record<string, unknown>>,
): boolean {
  return plan.some((row) =>
    Object.values(row).some(
      (value) => typeof value === "string" && value.includes("TEMP B-TREE"),
    ),
  );
}

export interface IndexerMetricsIndexReport {
  valid: boolean;
  present: string[];
  missing: string[];
}

/** Report which of the collector's indexes exist in the database. */
export function verifyIndexerMetricsIndexes(
  targetDb?: Database.Database,
): IndexerMetricsIndexReport {
  const database = targetDb || getDb();
  const existing = new Set(
    (
      database
        .prepare("SELECT name FROM sqlite_master WHERE type = 'index'")
        .all() as Array<{ name: string }>
    ).map((row) => row.name),
  );

  const present: string[] = [];
  const missing: string[] = [];
  for (const indexName of Object.values(INDEXER_METRICS_INDEXES)) {
    if (existing.has(indexName)) present.push(indexName);
    else missing.push(indexName);
  }

  return { valid: missing.length === 0, present, missing };
}

// ---------------------------------------------------------------------------
// In-memory queue locks for concurrent calls (#336)
// ---------------------------------------------------------------------------

/**
 * Persist one event row. Returns true when a new row was written and false
 * when the store already held it. Defaults to `insertEvent`, an
 * `INSERT OR IGNORE` against the events table.
 */
export type MetricsEventPersistFn = (event: EventRow) => boolean | Promise<boolean>;

export interface IndexerMetricsQueueOptions {
  persist?: MetricsEventPersistFn;
  maxQueueSize?: number;
  name?: string;
}

export interface MetricsEnqueueResult {
  queuedCount: number;
  duplicateCount: number;
}

export interface MetricsFlushResult {
  processedCount: number;
  insertedCount: number;
  duplicateCount: number;
}

export interface MetricsSubmitResult {
  queuedCount: number;
  insertedCount: number;
  duplicateCount: number;
}

/** Default ceiling on notifications held in memory. */
export const DEFAULT_METRICS_QUEUE_MAX_SIZE = 10_000;

export class IndexerMetricsQueueOverflowError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "IndexerMetricsQueueOverflowError";
  }
}

/** Identity of an event row, matching UNIQUE(contract_id, ledger_sequence, event_type). */
export function metricsEventIdentityKey(
  event: Pick<EventRow, "contractId" | "eventType" | "ledgerSequence">,
): string {
  return `${event.contractId}|${event.ledgerSequence}|${event.eventType}`;
}

/**
 * Bounded in-memory queue that serializes event inserts per event identity.
 *
 * Concurrent notifications routinely carry the same event – overlapping poll
 * windows, retried batches, several collectors in one process. Without a lock
 * two callers can both observe "not indexed yet" and both insert. Draining
 * every row under a lock keyed on its identity, and checking the persisted-key
 * set inside that lock, means exactly one caller writes each event while
 * unrelated events still persist concurrently.
 */
export class IndexerMetricsEventQueue {
  readonly name: string;
  readonly maxQueueSize: number;

  private readonly persist: MetricsEventPersistFn;
  private readonly pending: EventRow[] = [];
  private readonly pendingKeys = new Set<string>();
  private readonly persistedKeys = new Set<string>();
  private readonly lockTails = new Map<string, Promise<void>>();
  private readonly heldLocks = new Set<string>();
  private queueMutex: Promise<void> = Promise.resolve();

  constructor(options: IndexerMetricsQueueOptions = {}) {
    this.name = options.name ?? COLLECTOR_NAME;
    this.persist = options.persist ?? defaultPersistEvent;
    const maxQueueSize = options.maxQueueSize ?? DEFAULT_METRICS_QUEUE_MAX_SIZE;
    if (!Number.isInteger(maxQueueSize) || maxQueueSize < 1) {
      throw new IndexerMetricsQueueOverflowError(
        `maxQueueSize must be a positive integer, received ${String(maxQueueSize)}`,
      );
    }
    this.maxQueueSize = maxQueueSize;
  }

  /** Notifications waiting to be flushed. */
  get size(): number {
    return this.pending.length;
  }

  /** Identity locks held right now – exposed for concurrency assertions. */
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
    return this.persistedKeys.has(metricsEventIdentityKey(event));
  }

  reset(): void {
    this.pending.length = 0;
    this.pendingKeys.clear();
    this.persistedKeys.clear();
    this.lockTails.clear();
    this.heldLocks.clear();
    this.queueMutex = Promise.resolve();
  }

  /** Serialize mutations of the queue structure itself. */
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
   * Queue notifications, dropping any already queued or already persisted.
   * Throws `IndexerMetricsQueueOverflowError` past `maxQueueSize`.
   */
  async enqueue(events: EventRow[]): Promise<MetricsEnqueueResult> {
    return this.withQueueMutex(() => {
      let queuedCount = 0;
      let duplicateCount = 0;

      for (const event of events) {
        const key = metricsEventIdentityKey(event);
        if (this.pendingKeys.has(key) || this.persistedKeys.has(key)) {
          duplicateCount++;
          continue;
        }
        if (this.pending.length >= this.maxQueueSize) {
          throw new IndexerMetricsQueueOverflowError(
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

  /** Drain the queue, persisting each row under its own identity lock. */
  async flush(): Promise<MetricsFlushResult> {
    let processedCount = 0;
    let insertedCount = 0;
    let duplicateCount = 0;

    for (;;) {
      const next = await this.withQueueMutex(() => this.pending.shift());
      if (!next) break;

      const key = metricsEventIdentityKey(next);
      processedCount++;

      await this.withEventLock(key, async () => {
        try {
          if (this.persistedKeys.has(key)) {
            duplicateCount++;
            return;
          }
          const inserted = await this.persist(next);
          this.persistedKeys.add(key);
          if (inserted) insertedCount++;
          else duplicateCount++;
        } finally {
          this.pendingKeys.delete(key);
        }
      });
    }

    return { processedCount, insertedCount, duplicateCount };
  }

  /** Enqueue and flush in one step – the entry point for notifications. */
  async submit(events: EventRow[]): Promise<MetricsSubmitResult> {
    const enqueued = await this.enqueue(events);
    const flushed = await this.flush();

    const result: MetricsSubmitResult = {
      queuedCount: enqueued.queuedCount,
      insertedCount: flushed.insertedCount,
      duplicateCount: enqueued.duplicateCount + flushed.duplicateCount,
    };

    logger.debug("indexer_metrics_collector event queue submit", {
      collector: this.name,
      submitted: events.length,
      ...result,
    });

    return result;
  }
}

function defaultPersistEvent(event: EventRow): boolean {
  return insertEvent(
    event.contractId,
    event.eventType,
    event.ledgerSequence,
    event.timestamp,
    event.dataJson,
  );
}

let defaultQueue = new IndexerMetricsEventQueue();
let inFlightCollection: Promise<IndexerMetrics> | null = null;

/** The queue backing `recordEventNotifications`. */
export function getIndexerMetricsQueue(): IndexerMetricsEventQueue {
  return defaultQueue;
}

/** Drop queue and in-flight collection state. Intended for tests. */
export function resetIndexerMetricsCollectorState(): void {
  defaultQueue = new IndexerMetricsEventQueue();
  inFlightCollection = null;
}

/**
 * Index event notifications through the locked memory queue. Safe to call
 * concurrently: identical notifications collapse to a single insert (#336).
 */
export async function recordEventNotifications(
  events: EventRow[],
): Promise<MetricsSubmitResult> {
  return defaultQueue.submit(events);
}

/**
 * Collect metrics with single-flight de-duplication: concurrent callers share
 * one snapshot instead of racing several transactions against the same tables.
 *
 * Any queued notifications are drained first, so the snapshot reflects every
 * event that has been handed to the collector.
 */
export async function collectIndexerMetricsAsync(
  targetDb?: Database.Database,
): Promise<IndexerMetrics> {
  if (inFlightCollection) return inFlightCollection;

  const collection = (async () => {
    await defaultQueue.flush();
    return collectIndexerMetrics(targetDb);
  })();

  inFlightCollection = collection;
  try {
    return await collection;
  } finally {
    if (inFlightCollection === collection) {
      inFlightCollection = null;
    }
  }
}

// ---------------------------------------------------------------------------
// Collection
// ---------------------------------------------------------------------------

/**
 * Collect indexer metrics using transaction isolation to ensure
 * a consistent snapshot across all metrics queries.
 */
export function collectIndexerMetrics(targetDb?: Database.Database): IndexerMetrics {
  const database = targetDb || getDb();

  // Execute all metric queries within a database transaction for isolation
  const getMetricsTx = database.transaction(() => {
    const lastLedgerRow = database
      .prepare(INDEXER_METRICS_QUERIES.lastLedger)
      .get() as { value: string } | undefined;
    const lastIndexedLedger = lastLedgerRow ? parseInt(lastLedgerRow.value, 10) : 0;

    const totalRow = database
      .prepare(INDEXER_METRICS_QUERIES.totalEvents)
      .get() as { count: number };

    const lastEventRow = database
      .prepare(INDEXER_METRICS_QUERIES.lastEventAt)
      .get() as { last_at: string | null };

    const typeRows = database
      .prepare(INDEXER_METRICS_QUERIES.eventsByType)
      .all() as Array<{ event_type: string; count: number }>;

    const eventsByType: Record<string, number> = {};
    for (const row of typeRows) {
      eventsByType[row.event_type] = row.count;
    }

    let activeContractsCount = 0;
    try {
      const activeContractsRow = database
        .prepare(INDEXER_METRICS_QUERIES.activeContracts)
        .get() as { count: number } | undefined;
      activeContractsCount = activeContractsRow ? activeContractsRow.count : 0;
    } catch {
      // monitored_contracts table might not exist yet
    }

    let totalSubscriptions = 0;
    try {
      const subscriptionsRow = database
        .prepare(INDEXER_METRICS_QUERIES.subscriptions)
        .get() as { count: number } | undefined;
      totalSubscriptions = subscriptionsRow ? subscriptionsRow.count : 0;
    } catch {
      // webhook_subscriptions table might not exist yet
    }

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

  return getMetricsTx();
}
