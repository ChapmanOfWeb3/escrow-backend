import { Server } from "@stellar/stellar-sdk/rpc";
import { scValToNative } from "@stellar/stellar-sdk";
import {
  getLastIndexedLedger,
  insertEventBatch,
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
      filters: [
        {
          type: "contract",
          contractIds,
          topics: [[...EVENT_TYPES]],
        },
      ],
      limit: 100,
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
