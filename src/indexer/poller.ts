import { Server } from "@stellar/stellar-sdk/rpc";
import { scValToNative } from "@stellar/stellar-sdk";
import {
  getLastIndexedLedger,
  insertEventBatch,
  getActiveContractIds,
  registerContract,
  adjustPollerInterval,
  getCurrentPollIntervalMs,
  verifySchemaUpToDate,
  assertSchemaValid,
  type EventRow,
} from "./db.js";
import { deliverWebhooks } from "./webhook-delivery.js";
import logger from "../utils/logger.js";

const RPC_URL =
  process.env.SOROBAN_RPC_URL || "https://soroban-testnet.stellar.org";
const server = new Server(RPC_URL);

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

const EVENT_TYPES = [
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
];

/**
 * Poll events for all active contract IDs stored in monitored_contracts (#85).
 * All events fetched in a single poll are written atomically together with the
 * ledger pointer update (#84) – so a mid-poll crash cannot advance the pointer
 * without committing the accompanying events.
 *
 * Returns whether the ledger actually advanced, so startPoller() can throttle
 * its polling frequency up or down based on ledger processing load (#274).
 */
export async function pollEvents(): Promise<boolean> {
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
    const events = await server.getEvents({
      startLedger,
      filters: [
        {
          type: "contract",
          contractIds,
          topics: [[...EVENT_TYPES]],
        },
      ],
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
    const batch: EventRow[] = events.events.map((event) => ({
      contractId: event.contractId?.contractId() ?? contractIds[0],
      eventType: scValToNative(event.topic[0]) as string,
      ledgerSequence: event.ledger,
      timestamp: event.ledgerClosedAt
        ? Math.floor(new Date(event.ledgerClosedAt).getTime() / 1000)
        : Math.floor(Date.now() / 1000),
      dataJson: JSON.stringify(event.value),
    }));

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
}
