import { insertEvent, type EventRow } from "./db.js";
import logger from "../utils/logger.js";

/**
 * event_type_filter – topic parser that decides which contract events the
 * indexer accepts, plus the operational machinery around that decision.
 *
 * The RPC `getEvents` topic filter is a coarse server-side sieve; this module
 * is the authoritative client-side check. It owns four concerns:
 *
 *  1. Threshold warning alerts – tracks consecutive filter failures and
 *     stalled processing, escalating to `logger.error` once the configured
 *     counts are reached so a silently degrading indexer is visible.
 *  2. RPC event ingestion – parses simulated or live RPC notifications and
 *     writes the matching ones into the `events` schema.
 *  3. Memory queue locks – serialises concurrent inserts per event identity so
 *     overlapping notifications cannot produce duplicate rows.
 *  4. Dynamic polling intervals – widens the poll delay when the network is
 *     idle and narrows it under load, measured by events that actually pass
 *     the filter rather than raw RPC payload size.
 */

// ---------------------------------------------------------------------------
// Topic parsing and matching
// ---------------------------------------------------------------------------

/**
 * Canonical list of contract event topics the indexer accepts.
 * `poller.ts` imports this rather than keeping its own copy, so the RPC-side
 * topic filter and the client-side check can never drift apart.
 */
export const EVENT_TYPES = [
  "initialized",
  "funded",
  "delivered",
  "approved",
  "dispute_raised",
  "dispute_resolved",
  "partial_release",
  "auto_release_claimed",
  "token_whitelisted",
  "token_removed",
] as const;

export type EventType = (typeof EVENT_TYPES)[number];

const EVENT_TYPE_SET: ReadonlySet<string> = new Set(EVENT_TYPES);

/** Returns true when `value` is one of the accepted event topics. */
export function isKnownEventType(value: unknown): value is EventType {
  return typeof value === "string" && EVENT_TYPE_SET.has(value);
}

/**
 * Extracts the event type from a decoded topic array.
 *
 * The event type is always the first topic entry. Returns `null` for a
 * malformed topic (empty, non-array, non-string head) or an unrecognised
 * type, so callers can reject rather than persist an unknown row.
 */
export function parseEventTopic(topic: unknown): EventType | null {
  if (!Array.isArray(topic) || topic.length === 0) return null;
  const head = topic[0];
  return isKnownEventType(head) ? head : null;
}

/** A decoded RPC event notification, as produced by the poller or a test. */
export interface RpcEventNotification {
  contractId: string;
  /** Decoded topic array; the first entry carries the event type. */
  topic: unknown[];
  ledger: number;
  /** Unix seconds. Defaults to now when absent. */
  timestamp?: number;
  value: unknown;
}

export interface EventFilterResult {
  /** Notifications whose topic matched a known event type. */
  matched: EventRow[];
  /** Notifications rejected by the filter, with the reason. */
  rejected: Array<{ notification: RpcEventNotification; reason: string }>;
}

function toEventRow(
  notification: RpcEventNotification,
  eventType: EventType,
): EventRow {
  return {
    contractId: notification.contractId,
    eventType,
    ledgerSequence: notification.ledger,
    timestamp: notification.timestamp ?? Math.floor(Date.now() / 1000),
    dataJson: JSON.stringify(notification.value),
  };
}

/**
 * Splits a batch of RPC notifications into the ones the indexer accepts and
 * the ones it rejects. Never throws: a malformed notification is reported as
 * a rejection so one bad event cannot abort a whole poll.
 */
export function filterMatchingEvents(
  notifications: RpcEventNotification[],
): EventFilterResult {
  const matched: EventRow[] = [];
  const rejected: EventFilterResult["rejected"] = [];

  for (const notification of notifications) {
    if (!notification || typeof notification.contractId !== "string" || notification.contractId.length === 0) {
      rejected.push({ notification, reason: "missing contractId" });
      continue;
    }

    if (!Number.isInteger(notification.ledger) || notification.ledger < 0) {
      rejected.push({ notification, reason: "invalid ledger sequence" });
      continue;
    }

    const eventType = parseEventTopic(notification.topic);
    if (eventType === null) {
      rejected.push({ notification, reason: "unmatched event topic" });
      continue;
    }

    matched.push(toEventRow(notification, eventType));
  }

  return { matched, rejected };
}

// ---------------------------------------------------------------------------
// 1. Threshold warning alerts
// ---------------------------------------------------------------------------

/** Consecutive failures before the filter escalates to an error-level alert. */
export const CONSECUTIVE_ERROR_THRESHOLD = parseInt(
  process.env.EVENT_FILTER_ERROR_THRESHOLD || "3",
  10,
);

/** Time without a successful filter pass before a stall is reported. */
export function getStallThresholdMs(): number {
  return parseInt(process.env.EVENT_FILTER_STALL_THRESHOLD_MS || "120000", 10);
}

export interface EventFilterHealth {
  /** Failures since the last success. Reset to 0 on any success. */
  consecutiveErrors: number;
  /** Epoch millis of the last successful pass, or null if none yet. */
  lastSuccessAt: number | null;
  /** Message from the most recent failure, or null. */
  lastError: string | null;
  /** True once `consecutiveErrors` has reached the configured threshold. */
  alerting: boolean;
}

let health: EventFilterHealth = {
  consecutiveErrors: 0,
  lastSuccessAt: null,
  lastError: null,
  alerting: false,
};

/** Read-only snapshot of filter health. */
export function getEventFilterHealth(): EventFilterHealth {
  return { ...health };
}

/** Clears health counters. Used between tests and after operator recovery. */
export function resetEventFilterHealth(): void {
  health = {
    consecutiveErrors: 0,
    lastSuccessAt: null,
    lastError: null,
    alerting: false,
  };
}

/**
 * Records a successful filter pass, clearing the consecutive-error counter and
 * any active alert.
 */
export function recordFilterSuccess(matchedCount = 0): EventFilterHealth {
  if (health.alerting) {
    logger.info("Event filter recovered – clearing alert state", {
      previousConsecutiveErrors: health.consecutiveErrors,
      matchedCount,
    });
  }

  health = {
    consecutiveErrors: 0,
    lastSuccessAt: Date.now(),
    lastError: null,
    alerting: false,
  };

  return { ...health };
}

/**
 * Records a filter failure. Once `CONSECUTIVE_ERROR_THRESHOLD` consecutive
 * failures have accumulated the module logs at error level on every further
 * failure, so the alert keeps firing while the condition persists rather than
 * going quiet after the first breach.
 */
export function recordFilterError(err: unknown): EventFilterHealth {
  const message = err instanceof Error ? err.message : String(err);

  health.consecutiveErrors += 1;
  health.lastError = message;

  if (health.consecutiveErrors >= CONSECUTIVE_ERROR_THRESHOLD) {
    health.alerting = true;
    logger.error("Event filter alert: consecutive error threshold reached", {
      consecutiveErrors: health.consecutiveErrors,
      threshold: CONSECUTIVE_ERROR_THRESHOLD,
      lastError: message,
      lastSuccessAt: health.lastSuccessAt,
    });
  } else {
    logger.warn("Event filter error recorded", {
      consecutiveErrors: health.consecutiveErrors,
      threshold: CONSECUTIVE_ERROR_THRESHOLD,
      error: message,
    });
  }

  return { ...health };
}

/**
 * Checks whether the filter has stalled – no successful pass within the
 * configured window – and logs an error-level alert when it has.
 *
 * Returns true when a stall was detected. A filter that has never succeeded
 * is not treated as stalled: there is no baseline to measure against yet.
 */
export function checkFilterStall(now: number = Date.now()): boolean {
  if (health.lastSuccessAt === null) return false;

  const stallThresholdMs = getStallThresholdMs();
  const elapsedMs = now - health.lastSuccessAt;

  if (elapsedMs > stallThresholdMs) {
    health.alerting = true;
    logger.error("Event filter alert: processing stalled", {
      elapsedMs,
      stallThresholdMs,
      consecutiveErrors: health.consecutiveErrors,
      lastSuccessAt: health.lastSuccessAt,
    });
    return true;
  }

  return false;
}

// ---------------------------------------------------------------------------
// 3. Memory queue locks
// ---------------------------------------------------------------------------

/**
 * Identity of an event row for locking purposes. Matches the
 * `UNIQUE(contract_id, ledger_sequence, event_type)` constraint on `events`,
 * so two notifications that the database would consider the same row are
 * serialised against each other.
 */
export function eventLockKey(event: EventRow): string {
  return `${event.contractId}|${event.ledgerSequence}|${event.eventType}`;
}

const inFlightByKey = new Map<string, Promise<unknown>>();

/** Number of event inserts currently held in the queue. */
export function getEventQueueDepth(): number {
  return inFlightByKey.size;
}

/** Clears the lock map. Used between tests. */
export function resetEventQueueForTests(): void {
  inFlightByKey.clear();
}

/**
 * Runs `fn` under a memory queue lock keyed by `key`.
 *
 * Callers contending for the same key are chained rather than run in
 * parallel, so a concurrent burst of identical notifications is applied one at
 * a time. The chain is built from the previous promise regardless of whether
 * it settled successfully, so one failed insert cannot deadlock the key. The
 * map entry is deleted once the last waiter drains, keeping the queue bounded
 * by live contention rather than by total events seen.
 */
export async function withEventQueueLock<T>(
  key: string,
  fn: () => Promise<T> | T,
): Promise<T> {
  const previous = inFlightByKey.get(key) ?? Promise.resolve();

  const run = previous.then(
    () => fn(),
    () => fn(),
  );

  // Store a settled-swallowing view so a rejection here never becomes an
  // unhandled rejection for the *next* waiter in the chain.
  const chained = run.then(
    () => undefined,
    () => undefined,
  );
  inFlightByKey.set(key, chained);

  try {
    return await run;
  } finally {
    if (inFlightByKey.get(key) === chained) {
      inFlightByKey.delete(key);
    }
  }
}

/**
 * Inserts one event under its queue lock.
 *
 * Returns true when this call created the row and false when it was already
 * present. `insertEvent` uses `INSERT OR IGNORE`, so the database is the final
 * authority on uniqueness; the lock exists so concurrent callers observe a
 * consistent inserted/skipped answer instead of racing.
 */
export async function enqueueEventInsert(event: EventRow): Promise<boolean> {
  return withEventQueueLock(eventLockKey(event), () =>
    insertEvent(
      event.contractId,
      event.eventType,
      event.ledgerSequence,
      event.timestamp,
      event.dataJson,
    ),
  );
}

export interface IngestResult {
  /** Rows newly written to the database. */
  inserted: number;
  /** Rows that matched the filter but already existed. */
  duplicates: number;
  /** Notifications the topic filter rejected. */
  rejected: number;
}

/**
 * Full ingest path: filter a batch of RPC notifications by topic, then insert
 * the matching rows under their queue locks.
 *
 * Safe to call concurrently with overlapping batches – identical events are
 * serialised by {@link withEventQueueLock} and deduplicated by the unique
 * constraint, so the same notification delivered twice yields one row.
 *
 * Updates filter health on both the success and failure paths so threshold
 * alerts reflect real ingest activity.
 */
export async function ingestRpcEvents(
  notifications: RpcEventNotification[],
): Promise<IngestResult> {
  try {
    const { matched, rejected } = filterMatchingEvents(notifications);

    if (rejected.length > 0) {
      logger.debug("Event filter rejected notifications", {
        rejectedCount: rejected.length,
        reasons: rejected.map((r) => r.reason),
      });
    }

    const outcomes = await Promise.all(
      matched.map((event) => enqueueEventInsert(event)),
    );

    const inserted = outcomes.filter(Boolean).length;

    recordFilterSuccess(matched.length);

    return {
      inserted,
      duplicates: outcomes.length - inserted,
      rejected: rejected.length,
    };
  } catch (err) {
    recordFilterError(err);
    throw err;
  }
}

// ---------------------------------------------------------------------------
// 4. Dynamic polling frequency intervals
// ---------------------------------------------------------------------------

const BASE_INTERVAL_MS = parseInt(
  process.env.EVENT_FILTER_POLL_INTERVAL_MS || "15000",
  10,
);
const MIN_INTERVAL_MS = parseInt(
  process.env.EVENT_FILTER_MIN_INTERVAL_MS || "5000",
  10,
);
const MAX_INTERVAL_MS = parseInt(
  process.env.EVENT_FILTER_MAX_INTERVAL_MS || "60000",
  10,
);
const IDLE_MULTIPLIER = parseFloat(
  process.env.EVENT_FILTER_IDLE_MULTIPLIER || "2",
);
const IDLE_THRESHOLD_CYCLES = parseInt(
  process.env.EVENT_FILTER_IDLE_THRESHOLD || "2",
  10,
);
const LOAD_DECREASE_FACTOR = parseFloat(
  process.env.EVENT_FILTER_LOAD_DECREASE_FACTOR || "0.5",
);

export interface FilterPollState {
  /** Delay to wait before the next poll. */
  currentIntervalMs: number;
  /** Matched events seen on the most recent cycle. */
  lastMatchedCount: number;
  /** Consecutive cycles that matched nothing. */
  idleCycles: number;
  /** Epoch millis of the last adjustment. */
  lastAdjustedAt: number;
}

function freshPollState(): FilterPollState {
  return {
    currentIntervalMs: BASE_INTERVAL_MS,
    lastMatchedCount: 0,
    idleCycles: 0,
    lastAdjustedAt: Date.now(),
  };
}

let pollState: FilterPollState = freshPollState();

/** Read-only snapshot of the polling state. */
export function getFilterPollState(): FilterPollState {
  return { ...pollState };
}

/** Current wait between polls, in milliseconds. */
export function getCurrentFilterIntervalMs(): number {
  return pollState.currentIntervalMs;
}

/** Resets polling to the base interval. */
export function resetFilterPollState(): void {
  pollState = freshPollState();
}

/**
 * Adjusts the poll delay from the number of events that passed the filter on
 * the last cycle.
 *
 * Load is measured in *matched* events rather than raw RPC results: a poll
 * that returns a large payload of events the filter discards is idle from the
 * indexer's point of view, and should back off like any other quiet cycle.
 *
 * - Idle: after {@link IDLE_THRESHOLD_CYCLES} consecutive empty cycles the
 *   delay grows by `IDLE_MULTIPLIER`, capped at `MAX_INTERVAL_MS`. The grace
 *   period keeps a single quiet cycle from immediately slowing the indexer.
 * - Under load: the delay contracts by `LOAD_DECREASE_FACTOR` toward
 *   `MIN_INTERVAL_MS`, and the idle counter resets.
 */
export function adjustFilterPollInterval(
  matchedEventCount: number,
): FilterPollState {
  pollState.lastMatchedCount = matchedEventCount;

  if (matchedEventCount <= 0) {
    pollState.idleCycles += 1;
    if (pollState.idleCycles >= IDLE_THRESHOLD_CYCLES) {
      pollState.currentIntervalMs = Math.min(
        Math.floor(pollState.currentIntervalMs * IDLE_MULTIPLIER),
        MAX_INTERVAL_MS,
      );
    }
  } else {
    pollState.idleCycles = 0;
    pollState.currentIntervalMs = Math.max(
      MIN_INTERVAL_MS,
      Math.floor(pollState.currentIntervalMs * LOAD_DECREASE_FACTOR),
    );
  }

  pollState.lastAdjustedAt = Date.now();
  return { ...pollState };
}
