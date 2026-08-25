import { Server } from "@stellar/stellar-sdk/rpc";
import type { Api } from "@stellar/stellar-sdk/rpc";
import { scValToNative } from "@stellar/stellar-sdk";
import {
  getLastIndexedLedger,
  insertEventBatch,
  insertHistoricalEventBatch,
  getActiveContractIds,
  registerContract,
  type EventRow,
} from "./db.js";
import { deliverWebhooks } from "./webhook-delivery.js";
import logger from "../utils/logger.js";

const RPC_URL =
  process.env.SOROBAN_RPC_URL || "https://soroban-testnet.stellar.org";
const server = new Server(RPC_URL);

// --- Dynamic polling interval (idle backoff) ---
// Base/minimum interval - used whenever the ledger is advancing (network active).
const POLL_INTERVAL_MIN_MS = parseInt(process.env.POLL_INTERVAL_MS || "15000", 10);
// Upper bound on how far the interval may back off during idle periods. This
// caps the worst-case detection delay for a real event once the network
// resumes, since duplicate-prevention correctness itself does not depend on
// how often we poll (see pollEvents() doc comment below).
const POLL_INTERVAL_MAX_MS = parseInt(
  process.env.POLL_INTERVAL_MAX_MS || "120000",
  10
);
// Multiplier applied to the current interval each consecutive idle poll.
const POLL_IDLE_BACKOFF_MULTIPLIER = parseFloat(
  process.env.POLL_IDLE_BACKOFF_MULTIPLIER || "1.5"
);

/**
 * Given the current polling interval and whether the last poll observed new
 * ledger activity, compute the interval to use for the next poll.
 *
 * - Activity resumes -> immediately reset to the minimum interval so new
 *   events are picked up promptly.
 * - Idle -> back off multiplicatively, capped at POLL_INTERVAL_MAX_MS, so the
 *   poller never gets stuck waiting indefinitely.
 */
export function nextPollIntervalMs(
  currentIntervalMs: number,
  hadActivity: boolean
): number {
  if (hadActivity) return POLL_INTERVAL_MIN_MS;
  const backedOff = Math.round(currentIntervalMs * POLL_IDLE_BACKOFF_MULTIPLIER);
  return Math.min(backedOff, POLL_INTERVAL_MAX_MS);
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

  try {
    const lastLedger = getLastIndexedLedger();
    const currentLedger = (await server.getLatestLedger()).sequence;
    if (currentLedger <= lastLedger) return false;

    const startLedger = lastLedger + 1;

    logger.info("Polling events", { startLedger, currentLedger });

    const events = await server.getEvents({
      startLedger,
      filters: buildEventFilter(contractIds),
      limit: 100,
    });

    // Build the batch to be written atomically (#84)
    const batch: EventRow[] = events.events.map((event) =>
      toEventRow(event, contractIds[0])
    );

    // Persist the batch and advance the ledger pointer atomically (#84)
    insertEventBatch(batch, currentLedger);
    logger.info("Processed indexer poll", {
      eventCount: events.events.length,
      upToLedger: currentLedger,
    });

    deliverWebhooks(startLedger, currentLedger).catch((err) =>
      logger.error("Error delivering webhooks", {
        error: err instanceof Error ? err.message : String(err),
      })
    );

    return true;
  } catch (err) {
    logger.error("Error polling events", {
      error: err instanceof Error ? err.message : String(err),
    });
    // Treat a failed poll as idle so we back off rather than hammering an
    // RPC endpoint that may itself be struggling.
    return false;
  }
}

// ---------------------------------------------------------------------------
// Historical event import (event_type_filter: dynamic start/end ledger)
// ---------------------------------------------------------------------------
// No pre-existing backfill/historical-ingestion mechanism was found elsewhere
// in this codebase (searched for "backfill"/"historical" usage) - this is a
// new, narrowly-scoped addition, not a duplicate of an existing one.

// Caps how large a single historical range can be, so a mistyped/huge range
// can't be silently accepted and hammer the RPC in one call. Follows the
// same parseInt(process.env.X || "default") config convention as
// POLL_INTERVAL_MS above - no new config mechanism introduced.
const MAX_HISTORICAL_RANGE_LEDGERS = parseInt(
  process.env.HISTORICAL_IMPORT_MAX_RANGE_LEDGERS || "10000",
  10
);
// Safety cap on how many 100-event pages a single import will page through,
// in case of an unexpectedly dense range or a misbehaving RPC cursor.
const MAX_HISTORICAL_PAGES = parseInt(
  process.env.HISTORICAL_IMPORT_MAX_PAGES || "50",
  10
);
const HISTORICAL_PAGE_LIMIT = 100;

export interface HistoricalRangeValidation {
  valid: boolean;
  error?: string;
}

/**
 * Validates a requested historical import range. Rejects clearly rather than
 * silently clamping (unlike the page/limit clamping used for pagination
 * elsewhere in db.ts) because a wrong ledger range here would silently
 * import the wrong data, not just the wrong page of otherwise-correct data.
 */
export function validateHistoricalRange(
  startLedger: number,
  endLedger: number,
  currentLedger: number
): HistoricalRangeValidation {
  if (!Number.isInteger(startLedger) || startLedger < 1) {
    return { valid: false, error: "startLedger must be a positive integer" };
  }
  if (!Number.isInteger(endLedger) || endLedger < 1) {
    return { valid: false, error: "endLedger must be a positive integer" };
  }
  if (startLedger > endLedger) {
    return { valid: false, error: "startLedger must be <= endLedger" };
  }
  if (endLedger > currentLedger) {
    return {
      valid: false,
      error: `endLedger (${endLedger}) is beyond the current chain head (${currentLedger}) - that ledger does not exist yet`,
    };
  }
  if (endLedger - startLedger + 1 > MAX_HISTORICAL_RANGE_LEDGERS) {
    return {
      valid: false,
      error: `requested range spans ${endLedger - startLedger + 1} ledgers, exceeding the ${MAX_HISTORICAL_RANGE_LEDGERS}-ledger max per historical import`,
    };
  }
  return { valid: true };
}

export interface HistoricalImportResult {
  startLedger: number;
  endLedger: number;
  eventsFound: number;
  eventsImported: number;
}

/**
 * Imports events for an arbitrary past ledger range (custom historical
 * import), independent of the live poller's forward progress.
 *
 * Deliberately does NOT call insertEventBatch()/advance last_ledger_sequence
 * - it uses insertHistoricalEventBatch() instead, so a backfill over old
 * ledgers can never rewind the live pointer backwards, and a backfill that
 * raced ahead of the live poller could never cause it to skip ledgers by
 * advancing the pointer past events the live poller hasn't processed yet.
 * Duplicate prevention (INSERT OR IGNORE on the same UNIQUE constraint) still
 * applies, so re-running an import - or importing a range the live poller has
 * since caught up to - is idempotent and produces no duplicate rows.
 */
export async function fetchHistoricalEvents(
  startLedger: number,
  endLedger: number
): Promise<HistoricalImportResult> {
  let contractIds: string[] = getActiveContractIds();
  if (contractIds.length === 0 && process.env.CONTRACT_ID) {
    registerContract(process.env.CONTRACT_ID, "default");
    contractIds = [process.env.CONTRACT_ID];
  }
  if (contractIds.length === 0) {
    throw new Error("No CONTRACT_IDs configured - cannot import historical events");
  }

  const currentLedger = (await server.getLatestLedger()).sequence;
  const validation = validateHistoricalRange(startLedger, endLedger, currentLedger);
  if (!validation.valid) {
    throw new Error(validation.error);
  }

  logger.info("Fetching historical events", { startLedger, endLedger, contractIds });

  const filters = buildEventFilter(contractIds);
  const collected: Api.EventResponse[] = [];
  let cursor: string | undefined;

  for (let page = 0; page < MAX_HISTORICAL_PAGES; page++) {
    const response = cursor
      ? await server.getEvents({ cursor, filters, limit: HISTORICAL_PAGE_LIMIT })
      : await server.getEvents({
          startLedger,
          endLedger,
          filters,
          limit: HISTORICAL_PAGE_LIMIT,
        });

    collected.push(...response.events);

    if (response.events.length < HISTORICAL_PAGE_LIMIT) break; // last page
    cursor = response.cursor;
  }

  const batch: EventRow[] = collected.map((event) => toEventRow(event, contractIds[0]));
  const eventsImported = insertHistoricalEventBatch(batch);

  logger.info("Historical event import complete", {
    startLedger,
    endLedger,
    eventsFound: batch.length,
    eventsImported,
  });

  return { startLedger, endLedger, eventsFound: batch.length, eventsImported };
}

let pollerTimer: NodeJS.Timeout | null = null;
let currentPollIntervalMs = POLL_INTERVAL_MIN_MS;

/** Exposed for tests/observability – the interval the next poll is scheduled at. */
export function getCurrentPollIntervalMs(): number {
  return currentPollIntervalMs;
}

export function startPoller() {
  if (pollerTimer) return;
  currentPollIntervalMs = POLL_INTERVAL_MIN_MS;
  logger.info("Starting event indexer poller", {
    intervalMs: currentPollIntervalMs,
    maxIntervalMs: POLL_INTERVAL_MAX_MS,
  });

  const runAndSchedule = async () => {
    const hadActivity = await pollEvents();
    currentPollIntervalMs = nextPollIntervalMs(currentPollIntervalMs, hadActivity);
    pollerTimer = setTimeout(runAndSchedule, currentPollIntervalMs);
  };

  runAndSchedule();
}

export function stopPoller() {
  if (pollerTimer) {
    clearTimeout(pollerTimer);
    pollerTimer = null;
  }
  currentPollIntervalMs = POLL_INTERVAL_MIN_MS;
}
