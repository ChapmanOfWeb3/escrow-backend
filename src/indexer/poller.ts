import { Server } from "@stellar/stellar-sdk/rpc";
import type { Api } from "@stellar/stellar-sdk/rpc";
import { scValToNative } from "@stellar/stellar-sdk";
import {
  getLastIndexedLedger,
  insertEventBatch,
  insertHistoricalEventBatch,
  getActiveContractIds,
  registerContract,
  adjustPollerInterval,
  getCurrentPollIntervalMs,
  resetPollerThrottleState,
  verifySchemaUpToDate,
  assertSchemaValid,
  type EventRow,
} from "./db.js";
import { RpcPollerClient } from "./rpc-poller-client.js";
import { deliverWebhooks } from "./webhook-delivery.js";
import { fetchEventsWithRetry } from "./event_type_filter.js";
import logger from "../utils/logger.js";

const RPC_URL =
  process.env.SOROBAN_RPC_URL || "https://soroban-testnet.stellar.org";

// ---------------------------------------------------------------------------
// RPC exponential backoff retry (#249)
// ---------------------------------------------------------------------------
// All RPC calls in the indexer poll loop go through RpcPollerClient, which
// retries transient failures (timeouts, connection resets, rate limits, 5xx)
// with a doubling backoff up to maxRetries, then resets on success.
const rpcClient = new RpcPollerClient(RPC_URL, {
  maxRetries: parseInt(process.env.INDEXER_RPC_MAX_RETRIES || "5", 10),
  initialBackoffMs: parseInt(
    process.env.INDEXER_RPC_INITIAL_BACKOFF_MS || "1000",
    10,
  ),
  backoffMultiplier: parseInt(
    process.env.INDEXER_RPC_BACKOFF_MULTIPLIER || "2",
    10,
  ),
  maxBackoffMs: parseInt(
    process.env.INDEXER_RPC_MAX_BACKOFF_MS || "30000",
    10,
  ),
});

// ---------------------------------------------------------------------------
// Failure and stall tracking for the poll loop (#253, #271)
// ---------------------------------------------------------------------------
// Consecutive failures escalate to an alert once the threshold is reached, and
// a poll loop that stops succeeding for longer than the stall threshold is
// reported even while individual polls keep "working".

const DEFAULT_FAILURE_THRESHOLD = 3;
const DEFAULT_STALL_THRESHOLD_MS = 120_000;

function readPositiveIntEnv(names: string[], fallback: number): number {
  for (const name of names) {
    const raw = process.env[name];
    if (raw === undefined) continue;
    const parsed = parseInt(raw, 10);
    if (Number.isFinite(parsed) && parsed > 0) return parsed;
  }
  return fallback;
}

class IndexerRunnerFailureMonitor {
  private consecutiveFailures = 0;
  private lastSuccessfulAt: number | null = null;
  private stallAlerted = false;

  get stallThresholdMs(): number {
    return readPositiveIntEnv(
      ["INDEXER_RUNNER_STALL_THRESHOLD_MS", "POLLER_STALL_THRESHOLD_MS"],
      DEFAULT_STALL_THRESHOLD_MS,
    );
  }

  getFailureThreshold(): number {
    return readPositiveIntEnv(
      ["INDEXER_RUNNER_FAILURE_THRESHOLD", "POLLER_FAILURE_THRESHOLD"],
      DEFAULT_FAILURE_THRESHOLD,
    );
  }

  getConsecutiveFailures(): number {
    return this.consecutiveFailures;
  }

  getLastSuccessfulAt(): number | null {
    return this.lastSuccessfulAt;
  }

  recordSuccess(): void {
    this.consecutiveFailures = 0;
    this.lastSuccessfulAt = Date.now();
    this.stallAlerted = false;
  }

  recordFailure(): number {
    this.consecutiveFailures += 1;
    return this.consecutiveFailures;
  }

  /** Warn once per stall episode when no poll has succeeded in the window. */
  checkStall(): void {
    if (this.lastSuccessfulAt === null) return;
    const elapsed = Date.now() - this.lastSuccessfulAt;
    if (elapsed <= this.stallThresholdMs) return;
    if (this.stallAlerted) return;

    this.stallAlerted = true;
    logger.warn("Poller stall detected – no successful poll for threshold period", {
      elapsedMs: elapsed,
      stallThresholdMs: this.stallThresholdMs,
      consecutiveFailures: this.consecutiveFailures,
    });
  }

  /** Reset in place – callers hold a reference to this instance. */
  reset(): void {
    this.consecutiveFailures = 0;
    this.lastSuccessfulAt = null;
    this.stallAlerted = false;
  }
}

const failureMonitor = new IndexerRunnerFailureMonitor();

export function getConsecutiveFailures(): number {
  return failureMonitor.getConsecutiveFailures();
}

export function getLastSuccessfulPollAt(): number | null {
  return failureMonitor.getLastSuccessfulAt();
}

export function resetFailureState(): void {
  failureMonitor.reset();
}

// ---------------------------------------------------------------------------
// Dynamic poll interval (#265)
// ---------------------------------------------------------------------------

const POLL_INTERVAL_MIN_MS = parseInt(process.env.POLL_INTERVAL_MS || "15000", 10);
const POLL_INTERVAL_MAX_MS = parseInt(
  process.env.POLL_INTERVAL_MAX_MS || "120000",
  10,
);
const POLL_INTERVAL_BACKOFF = 2;

/**
 * Next poll delay given the current one and whether the last poll saw activity.
 *
 * Idle polls back off geometrically up to POLL_INTERVAL_MAX_MS; the first
 * active poll drops straight back to the minimum. Pure function so the backoff
 * curve can be reasoned about (and tested) without running the loop.
 */
export function nextPollIntervalMs(
  currentIntervalMs: number,
  sawActivity: boolean,
): number {
  if (sawActivity) return POLL_INTERVAL_MIN_MS;
  return Math.min(currentIntervalMs * POLL_INTERVAL_BACKOFF, POLL_INTERVAL_MAX_MS);
}

/** The interval the poll loop is currently using. */
export { getCurrentPollIntervalMs };

/** Map an RPC event notification onto the row shape `events` stores. */
function toEventRow(event: any, fallbackContractId: string): EventRow {
  return {
    contractId: event.contractId?.contractId?.() ?? fallbackContractId,
    eventType: scValToNative(event.topic[0]) as string,
    ledgerSequence: event.ledger,
    timestamp: event.ledgerClosedAt
      ? Math.floor(new Date(event.ledgerClosedAt).getTime() / 1000)
      : Math.floor(Date.now() / 1000),
    dataJson: JSON.stringify(scValToNative(event.value)),
  };
}

// ---------------------------------------------------------------------------
// Poll diagnostics (#270)
// ---------------------------------------------------------------------------

const POLL_DIAGNOSTIC_LOG_MIN_INTERVAL_MS = parseInt(
  process.env.POLL_DIAGNOSTIC_LOG_MIN_INTERVAL_MS || "0",
  10,
);
let lastPollDiagnosticAt = 0;

/** Clear the diagnostics throttle so the next poll logs unconditionally. */
export function resetPollDiagnosticsThrottle(): void {
  lastPollDiagnosticAt = 0;
}

/**
 * Debug line carrying poll duration and payload size for one cycle.
 *
 * Throttled to at most one line per POLL_DIAGNOSTIC_LOG_MIN_INTERVAL_MS so a
 * fast poll loop cannot flood the log; the interval defaults to 0 (every poll).
 */
function logPollDiagnostics(elapsedMs: number, batch: EventRow[]): void {
  const now = Date.now();
  if (
    POLL_DIAGNOSTIC_LOG_MIN_INTERVAL_MS > 0 &&
    lastPollDiagnosticAt > 0 &&
    now - lastPollDiagnosticAt < POLL_DIAGNOSTIC_LOG_MIN_INTERVAL_MS
  ) {
    return;
  }
  lastPollDiagnosticAt = now;

  logger.debug(
    `Indexer poll diagnostics elapsedMs=${Math.round(elapsedMs)} ` +
      `payloadSizeBytes=${JSON.stringify(batch).length}`,
    {
      elapsedMs: Math.round(elapsedMs),
      payloadSizeBytes: JSON.stringify(batch).length,
      eventCount: batch.length,
    },
  );
}

// ---------------------------------------------------------------------------
// Historical range import (#254)
// ---------------------------------------------------------------------------

/** Upper bound on how many ledgers a single backfill may cover. */
export const MAX_LEDGERS_PER_IMPORT = parseInt(
  process.env.MAX_LEDGERS_PER_IMPORT || "10000",
  10,
);

const HISTORICAL_PAGE_SIZE = 100;

export interface HistoricalRangeValidation {
  valid: boolean;
  error?: string;
}

export interface HistoricalImportResult {
  eventsFound: number;
  eventsImported: number;
}

/**
 * Check a requested backfill window against the chain head and the per-import
 * ceiling. Pure, so callers can validate before opening any RPC connection.
 */
export function validateHistoricalRange(
  startLedger: number,
  endLedger: number,
  chainHeadLedger: number,
): HistoricalRangeValidation {
  if (!Number.isInteger(startLedger) || startLedger <= 0) {
    return { valid: false, error: "startLedger must be a positive integer" };
  }
  if (!Number.isInteger(endLedger) || endLedger <= 0) {
    return { valid: false, error: "endLedger must be a positive integer" };
  }
  if (startLedger > endLedger) {
    return { valid: false, error: "startLedger must be <= endLedger" };
  }
  if (endLedger > chainHeadLedger) {
    return {
      valid: false,
      error:
        `endLedger ${endLedger} does not exist yet – ` +
        `the chain head is ${chainHeadLedger}`,
    };
  }

  const span = endLedger - startLedger + 1;
  if (span > MAX_LEDGERS_PER_IMPORT) {
    return {
      valid: false,
      error:
        `range covers ${span} ledgers, exceeding the ` +
        `${MAX_LEDGERS_PER_IMPORT}-ledger maximum per import`,
    };
  }

  return { valid: true };
}

/**
 * Import a historical ledger window without disturbing the live poller.
 *
 * Rows go through `insertHistoricalEventBatch`, which keeps the live ledger
 * pointer where it is and relies on the same UNIQUE constraint as the poller —
 * so re-running an import is idempotent. Pages are followed by cursor until a
 * short page signals the range is fully collected.
 */
export async function fetchHistoricalEvents(
  startLedger: number,
  endLedger: number,
): Promise<HistoricalImportResult> {
  const chainHead = (await rpcClient.getLatestLedger()).sequence;
  const validation = validateHistoricalRange(startLedger, endLedger, chainHead);
  if (!validation.valid) {
    throw new Error(`Invalid historical range: ${validation.error}`);
  }

  let contractIds: string[] = getActiveContractIds();
  if (contractIds.length === 0 && process.env.CONTRACT_ID) {
    contractIds = [process.env.CONTRACT_ID];
  }

  let eventsFound = 0;
  let eventsImported = 0;
  let cursor: string | undefined;

  for (;;) {
    // Cursor mode supersedes the ledger window once paging has started; sending
    // both is rejected by the RPC.
    const params = cursor
      ? { cursor, limit: HISTORICAL_PAGE_SIZE }
      : { startLedger, endLedger, contractIds, limit: HISTORICAL_PAGE_SIZE };

    const page = await rpcClient.getEvents(params);
    const pageEvents: any[] = page?.events ?? [];
    if (pageEvents.length === 0) break;

    eventsFound += pageEvents.length;

    const batch: EventRow[] = pageEvents.map((event) =>
      toEventRow(event, contractIds[0] ?? ""),
    );
    const result = insertHistoricalEventBatch(batch, { startLedger, endLedger });
    eventsImported += result.inserted;

    cursor = page?.cursor;
    if (pageEvents.length < HISTORICAL_PAGE_SIZE || !cursor) break;
  }

  logger.info("Historical import complete", {
    startLedger,
    endLedger,
    eventsFound,
    eventsImported,
  });

  return { eventsFound, eventsImported };
}

// ---------------------------------------------------------------------------
// Memory queue lock for concurrent event inserts (#251)
// ---------------------------------------------------------------------------
// A single promise chain acts as an in-memory queue so overlapping
// indexer_runner executions serialize their writes to the events table and the
// ledger pointer. Without this, concurrent notifications could interleave the
// atomic batch + pointer update and produce conflicting/duplicate entries.
let eventInsertTail: Promise<void> = Promise.resolve();

/**
 * Reset the in-memory event-insert queue lock (used by tests).
 */
export function resetEventInsertQueueForTests(): void {
  eventInsertTail = Promise.resolve();
}

/**
 * Enqueue an event batch write behind a memory queue lock.
 *
 * Concurrent callers are chained onto a single promise tail and executed one at
 * a time, preserving the atomicity of `insertEventBatch` and preventing
 * duplicate or conflicting inserts from overlapping iterations.
 */
export function enqueueEventInsert(
  events: EventRow[],
  newLedger: number,
): Promise<void> {
  const run = eventInsertTail.then(() => {
    insertEventBatch(events, newLedger);
  });
  // Keep the queue alive even if an insert rejects so later enqueues proceed.
  eventInsertTail = run.then(
    () => undefined,
    () => undefined,
  );
  return run;
}

/**
 * Poll events for all active contract IDs stored in monitored_contracts (#85).
 * All events fetched in a single poll are written atomically together with the
 * ledger pointer update (#84) – so a mid-poll crash cannot advance the pointer
 * without committing the accompanying events.
 *
 * Returns whether the network showed activity (a new ledger closed since the
 * last poll). The caller uses this to drive the dynamic polling interval
 * (see nextPollIntervalMs()) – NOTE this only affects how *often* we poll,
 * never *what* gets written. Duplicate prevention itself is guaranteed by the
 * UNIQUE(contract_id, ledger_sequence, event_type) constraint + INSERT OR
 * IGNORE in db.ts, and every poll always resumes from the last committed
 * ledger pointer (lastLedger + 1) regardless of how much time has passed
 * since the previous poll. So a longer interval can only delay *when* an
 * event is detected – it cannot cause an event to be missed, double-counted,
 * or processed out of order.
 */
export async function pollEvents(): Promise<boolean> {
  const pollStartedAt = performance.now();

  // --- Resolve active contract IDs from the DB (#85) ---
  let contractIds: string[] = getActiveContractIds();

  // Fall back to the legacy single CONTRACT_ID env var so existing deployments
  // keep working without any DB seed step.
  if (contractIds.length === 0 && process.env.CONTRACT_ID) {
    registerContract(process.env.CONTRACT_ID, "default");
    contractIds = [process.env.CONTRACT_ID];
  }

  if (contractIds.length === 0) {
    logger.debug("No CONTRACT_IDs configured – skipping indexer poll");
    return false;
  }

  // --- Alerting: stall detection before polling (#253, #271) ---
  failureMonitor.checkStall();
  if (failureMonitor.getLastSuccessfulAt()) {
    const elapsed = Date.now() - (failureMonitor.getLastSuccessfulAt() as number);
    const stallThresholdMs = parseInt(
      process.env.INDEXER_RUNNER_STALL_THRESHOLD_MS ||
        process.env.POLLER_STALL_THRESHOLD_MS ||
        String(failureMonitor.stallThresholdMs),
      10,
    );
    logger.debug("Poller stall diagnostics", {
      elapsedMsSinceLastSuccess: elapsed,
      stallThresholdMs,
    });
  }

  const pollStart = performance.now();

  try {
    // Validate the schema before matching events against EVENT_TYPES – a stale
    // schema must not silently pass through the topic filter (#282).
    verifySchemaUpToDate();

    const lastLedger = getLastIndexedLedger();
    const currentLedger = (await rpcClient.getLatestLedger()).sequence;
    if (currentLedger <= lastLedger) {
      // --- Dynamic throttling: idle cycle (#265) ---
      adjustPollerInterval(0);
      return false;
    }

    const startLedger = lastLedger + 1;

    logger.info("Polling events", { startLedger, currentLedger });

    const eventsStart = performance.now();
    const events = await fetchEventsWithRetry(rpcClient.rpcServer, {
      startLedger,
      contractIds,
      limit: 100,
    });
    const eventsElapsed = performance.now() - eventsStart;

    // --- Diagnostics: payload size and timing (#270) ---
    const payloadSizeBytes = JSON.stringify(events.events).length;
    logger.debug("RPC getEvents diagnostics", {
      elapsedMs: Math.round(eventsElapsed),
      payloadSizeBytes,
      eventCount: events.events.length,
      startLedger,
      currentLedger,
    });

    // Build the batch to be written atomically (#84)
    const batch: EventRow[] = events.events.map((event) =>
      toEventRow(event, contractIds[0])
    );

    // Persist the batch and advance the ledger pointer atomically (#84).
    // Concurrent indexer_runner executions share a memory queue lock so their
    // event inserts are serialized and cannot conflict or duplicate entries (#251).
    await enqueueEventInsert(batch, currentLedger);

    const totalElapsed = performance.now() - pollStart;
    failureMonitor.recordSuccess();

    // --- Dynamic poller throttling (#265) ---
    const throttleState = adjustPollerInterval(events.events.length);

    logger.info("Processed indexer poll", {
      eventCount: events.events.length,
      upToLedger: currentLedger,
      elapsedMs: Math.round(totalElapsed),
      pollIntervalMs: throttleState.currentIntervalMs,
    });

    logPollDiagnostics(performance.now() - pollStartedAt, batch);

    deliverWebhooks(startLedger, currentLedger).catch((err) =>
      logger.error("Error delivering webhooks", {
        error: err instanceof Error ? err.message : String(err),
      })
    );

    return true;
  } catch (err) {
    const totalElapsed = performance.now() - pollStart;
    const consecutiveFailures = failureMonitor.recordFailure();

    logger.error("Error polling events", {
      error: err instanceof Error ? err.message : String(err),
      consecutiveFailures,
      elapsedMs: Math.round(totalElapsed),
    });

    // Keep legacy poller alert message for existing #271 tests
    if (
      failureMonitor.getConsecutiveFailures() >=
      failureMonitor.getFailureThreshold()
    ) {
      logger.error("Poller alert: consecutive failure threshold exceeded", {
        consecutiveFailures: failureMonitor.getConsecutiveFailures(),
        threshold: failureMonitor.getFailureThreshold(),
        lastSuccessAt: failureMonitor.getLastSuccessfulAt(),
      });
    }

    // A failed poll advanced nothing, so report no progress to the throttler.
    return false;
  }
}

let pollerTimeout: NodeJS.Timeout | null = null;
let pollerRunning = false;

async function pollLoop() {
  if (!pollerRunning) return;
  await pollEvents();
  const interval = getCurrentPollIntervalMs();
  pollerTimeout = setTimeout(pollLoop, interval);
}

export function startPoller() {
  if (pollerRunning) return;

  // Migration verification hook: fail-fast on startup if the database schema is
  // out of sync, so the indexer cannot start against a stale schema (#255).
  assertSchemaValid();

  pollerRunning = true;
  logger.info("Starting event indexer poller", {
    intervalMs: getCurrentPollIntervalMs(),
  });
  pollEvents();
  pollerTimeout = setTimeout(pollLoop, getCurrentPollIntervalMs());
}

export function stopPoller() {
  pollerRunning = false;
  if (pollerTimeout) {
    clearTimeout(pollerTimeout);
    pollerTimeout = null;
  }
  resetPollerThrottleState();
}
