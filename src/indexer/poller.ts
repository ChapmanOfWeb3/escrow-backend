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
  verifySchemaUpToDate,
  assertSchemaValid,
  type EventRow,
} from "./db.js";
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

const failureMonitor = getIndexerRunnerFailureMonitor();

export function getConsecutiveFailures(): number {
  return failureMonitor.getConsecutiveFailures();
}

export function getLastSuccessfulPollAt(): number | null {
  return failureMonitor.getLastSuccessfulAt();
}

export function resetFailureState(): void {
  resetIndexerRunnerFailureState();
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
    const currentLedger = (await server.getLatestLedger()).sequence;
    if (currentLedger <= lastLedger) {
      // --- Dynamic throttling: idle cycle (#265) ---
      adjustPollerInterval(0);
      return false;
    }

    const startLedger = lastLedger + 1;

    logger.info("Polling events", { startLedger, currentLedger });

    const eventsStart = performance.now();
    const events = await fetchEventsWithRetry(server, {
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
    consecutiveFailures = 0;
    lastSuccessfulPollAt = Date.now();

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
    consecutiveFailures += 1;

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
  currentPollIntervalMs = POLL_INTERVAL_MIN_MS;
}
