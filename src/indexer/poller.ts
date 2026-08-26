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
  type EventRow,
} from "./db.js";
import { deliverWebhooks } from "./webhook-delivery.js";
import logger from "../utils/logger.js";

const RPC_URL =
  process.env.SOROBAN_RPC_URL || "https://soroban-testnet.stellar.org";
const server = new Server(RPC_URL);

// ---------------------------------------------------------------------------
// Alerting thresholds (#271)
// ---------------------------------------------------------------------------
const CONSECUTIVE_FAILURE_THRESHOLD = parseInt(
  process.env.POLLER_FAILURE_THRESHOLD || "3",
  10,
);

function getStallThresholdMs(): number {
  return parseInt(process.env.POLLER_STALL_THRESHOLD_MS || "120000", 10);
}

let consecutiveFailures = 0;
let lastSuccessfulPollAt: number | null = null;

export function getConsecutiveFailures(): number {
  return consecutiveFailures;
}

export function getLastSuccessfulPollAt(): number | null {
  return lastSuccessfulPollAt;
}

export function resetFailureState(): void {
  consecutiveFailures = 0;
  lastSuccessfulPollAt = null;
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

function buildEventFilter(contractIds: string[]): Api.EventFilter[] {
  return [
    {
      type: "contract",
      contractIds,
      topics: [[...EVENT_TYPES]],
    },
  ];
}

function toEventRow(event: Api.EventResponse, fallbackContractId: string): EventRow {
  return {
    contractId: event.contractId?.contractId() ?? fallbackContractId,
    eventType: scValToNative(event.topic[0]) as string,
    ledgerSequence: event.ledger,
    timestamp: event.ledgerClosedAt
      ? Math.floor(new Date(event.ledgerClosedAt).getTime() / 1000)
      : Math.floor(Date.now() / 1000),
    dataJson: JSON.stringify(event.value),
  };
}

// --- High-frequency diagnostic logging (poll speed + payload sizes) ---
// event_type_filter's decoded event payloads (dataJson) can contain job
// participant Stellar addresses (client/freelancer/arbiter) and amounts -
// see db.ts's getJobsByWallet(), which extracts exactly those fields from
// this same column. Debug logging here records payload *sizes* only, never
// the raw dataJson content, so this can't leak that data through logs.
//
// logger.debug() is already off by default in production (logger.ts:
// LOG_LEVEL defaults to "info" when NODE_ENV=production, "debug"
// otherwise) - that's the primary gate. On top of that, no log-sampling/
// rate-limiting convention existed anywhere in this codebase for a
// per-poll-cycle log line, so this adds a simple time-based throttle
// (independent of POLL_INTERVAL_MS) as a ceiling against a misconfigured,
// very short poll interval turning this into unconditional hot-path
// logging.
const POLL_DIAGNOSTIC_LOG_MIN_INTERVAL_MS = parseInt(
  process.env.POLL_DIAGNOSTIC_LOG_MIN_INTERVAL_MS || "5000",
  10
);
let lastDiagnosticLogAt = 0;

/** Exposed for tests - resets the diagnostic-log throttle so each test starts fresh. */
export function resetPollDiagnosticsThrottle(): void {
  lastDiagnosticLogAt = 0;
}

function logPollDiagnostics(elapsedMs: number, batch: EventRow[]): void {
  const now = Date.now();
  if (now - lastDiagnosticLogAt < POLL_DIAGNOSTIC_LOG_MIN_INTERVAL_MS) return;
  lastDiagnosticLogAt = now;

  const payloadSizes = batch.map((ev) => Buffer.byteLength(ev.dataJson, "utf8"));
  const totalPayloadBytes = payloadSizes.reduce((sum, n) => sum + n, 0);
  const avgPayloadBytes = payloadSizes.length
    ? Math.round(totalPayloadBytes / payloadSizes.length)
    : 0;

  logger.debug(
    `Poll diagnostics: elapsedMs=${elapsedMs.toFixed(1)} eventCount=${batch.length} ` +
      `totalPayloadBytes=${totalPayloadBytes} avgPayloadBytes=${avgPayloadBytes}`,
    { elapsedMs, eventCount: batch.length, totalPayloadBytes, avgPayloadBytes }
  );
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

  // --- Diagnostics: stall detection before polling (#270, #271) ---
  if (lastSuccessfulPollAt) {
    const stallThresholdMs = getStallThresholdMs();
    const elapsed = Date.now() - lastSuccessfulPollAt;
    if (elapsed > stallThresholdMs) {
      logger.warn("Poller stall detected – no successful poll for threshold period", {
        elapsedMs: elapsed,
        stallThresholdMs,
        consecutiveFailures,
      });
    }
    logger.debug("Poller stall diagnostics", {
      elapsedMsSinceLastSuccess: elapsed,
      stallThresholdMs,
    });
  }

  const pollStart = performance.now();

  try {
    const lastLedger = getLastIndexedLedger();
    const currentLedger = (await server.getLatestLedger()).sequence;
    if (currentLedger <= lastLedger) {
      // --- Dynamic throttling: idle cycle (#265) ---
      adjustPollerInterval(0);
      return;
    }

    const startLedger = lastLedger + 1;

    logger.info("Polling events", { startLedger, currentLedger });

    const eventsStart = performance.now();
    const events = await server.getEvents({
      startLedger,
      filters: buildEventFilter(contractIds),
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

    // Persist the batch and advance the ledger pointer atomically (#84)
    insertEventBatch(batch, currentLedger);

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

    // --- Alerting: warn when consecutive failures hit threshold (#271) ---
    if (consecutiveFailures >= CONSECUTIVE_FAILURE_THRESHOLD) {
      logger.error("Poller alert: consecutive failure threshold exceeded", {
        consecutiveFailures,
        threshold: CONSECUTIVE_FAILURE_THRESHOLD,
        lastSuccessAt: lastSuccessfulPollAt,
      });
    }
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
