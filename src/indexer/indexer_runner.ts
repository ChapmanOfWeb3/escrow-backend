import logger from "../utils/logger.js";

/**
 * Indexer runner – diagnostics helpers for the main indexer event poller (#252)
 * and the dynamic poller throttle parameters that size the poll wait delay (#256).
 *
 * High-frequency debug logs track poll speeds (elapsedMs) and payload sizes so
 * operators can spot slow RPC rounds or unexpectedly large event batches.
 */

export interface IndexerRunnerPollDiagnostics {
  operation: string;
  status: "started" | "success" | "failure";
  elapsedMs: number;
  payloadSizeBytes?: number;
  eventCount?: number;
  startLedger?: number;
  currentLedger?: number;
  error?: string;
}

export function payloadSizeBytes(value: unknown): number {
  return Buffer.byteLength(JSON.stringify(value ?? null), "utf8");
}

/**
 * Emit an indexer_runner poll diagnostics debug log. Always includes elapsedMs
 * so validation can assert timing fields are present (#252).
 */
export function logIndexerRunnerPollDiagnostics(
  diagnostics: IndexerRunnerPollDiagnostics,
): void {
  logger.debug("indexer_runner poll diagnostics", diagnostics);
}

// ---------------------------------------------------------------------------
// Dynamic poller throttling parameters (#256)
// ---------------------------------------------------------------------------
//
// The indexer_runner owns the poll throttling parameters that decide how long
// the poller waits between cycles based on ledger processing load:
//   - When the network is idle (no ledger activity / no events processed) the
//     wait delay backs off toward MAX_POLL_INTERVAL_MS so the RPC endpoint is
//     not hammered with pointless polls.
//   - When events are being processed the delay is pulled back toward
//     MIN_POLL_INTERVAL_MS so the indexer stays responsive under load.

export interface IndexerRunnerThrottleParameters {
  /** Starting delay in ms before any load is observed. */
  baseIntervalMs: number;
  /** Floor for the poll wait delay in ms. */
  minIntervalMs: number;
  /** Ceiling for the poll wait delay in ms after long idle periods. */
  maxIntervalMs: number;
  /** Factor applied to the delay on each idle backing-off step. */
  idleMultiplier: number;
  /** Consecutive idle cycles required before the delay starts backing off. */
  idleThresholdCycles: number;
  /** Factor applied to the delay on a loaded poll (must be < 1). */
  loadDecreaseFactor: number;
}

export interface IndexerRunnerThrottleState {
  /** Current effective poll wait delay in ms. */
  currentIntervalMs: number;
  /** Event count observed during the most recent poll adjustment. */
  lastProcessedEventCount: number;
  /** Consecutive idle (zero-event) polls so far. */
  idleCycles: number;
  /** Timestamp of the most recent throttle adjustment. */
  lastLoadAdjustmentAt: number;
}

const BASE_POLL_INTERVAL_MS = parseInt(
  process.env.INDEXER_RUNNER_POLL_INTERVAL_MS || "15000",
  10,
);
const MIN_POLL_INTERVAL_MS = parseInt(
  process.env.INDEXER_RUNNER_MIN_POLL_INTERVAL_MS || "5000",
  10,
);
const MAX_POLL_INTERVAL_MS = parseInt(
  process.env.INDEXER_RUNNER_MAX_POLL_INTERVAL_MS || "60000",
  10,
);
const IDLE_MULTIPLIER = parseFloat(
  process.env.INDEXER_RUNNER_IDLE_MULTIPLIER || "2",
);
const IDLE_THRESHOLD_CYCLES = parseInt(
  process.env.INDEXER_RUNNER_IDLE_THRESHOLD_CYCLES || "3",
  10,
);
const LOAD_DECREASE_FACTOR = parseFloat(
  process.env.INDEXER_RUNNER_LOAD_DECREASE_FACTOR || "0.5",
);

let runnerThrottleState: IndexerRunnerThrottleState = {
  currentIntervalMs: BASE_POLL_INTERVAL_MS,
  lastProcessedEventCount: 0,
  idleCycles: 0,
  lastLoadAdjustmentAt: Date.now(),
};

/** Snapshot of the configured throttle parameters (read-only). */
export function getIndexerRunnerThrottleParameters(): IndexerRunnerThrottleParameters {
  return {
    baseIntervalMs: BASE_POLL_INTERVAL_MS,
    minIntervalMs: MIN_POLL_INTERVAL_MS,
    maxIntervalMs: MAX_POLL_INTERVAL_MS,
    idleMultiplier: IDLE_MULTIPLIER,
    idleThresholdCycles: IDLE_THRESHOLD_CYCLES,
    loadDecreaseFactor: LOAD_DECREASE_FACTOR,
  };
}

/** Snapshot of the current throttle state (read-only copy). */
export function getIndexerRunnerThrottleState(): IndexerRunnerThrottleState {
  return { ...runnerThrottleState };
}

/** Reset the throttle state to defaults (useful for tests). */
export function resetIndexerRunnerThrottleState(): void {
  runnerThrottleState = {
    currentIntervalMs: BASE_POLL_INTERVAL_MS,
    lastProcessedEventCount: 0,
    idleCycles: 0,
    lastLoadAdjustmentAt: Date.now(),
  };
}

/** Poll wait delay the runner should use before the next cycle. */
export function getIndexerRunnerPollDelayMs(): number {
  return runnerThrottleState.currentIntervalMs;
}

/**
 * Adjust the poll wait delay based on the ledger processing load observed in
 * the most recent poll cycle (#256).
 *
 * A poll that processed zero events means the network is idle: once
 * `idleThresholdCycles` consecutive idle polls have been seen the wait delay
 * backs off (multiplied by `idleMultiplier`, capped at `maxIntervalMs`) so the
 * runner stops polling as frequently.
 *
 * A poll that processed any events means the network is active: idle cycles are
 * cleared and the delay is pulled back toward `minIntervalMs`.
 *
 * @param processedEventCount - Number of events handled in the last poll.
 * @returns Updated throttle state snapshot.
 */
export function adjustIndexerRunnerPollInterval(
  processedEventCount: number,
): IndexerRunnerThrottleState {
  const state = runnerThrottleState;
  state.lastProcessedEventCount = processedEventCount;

  if (processedEventCount === 0) {
    // Idle network → the polling wait delay increases once enough consecutive
    // idle cycles have been observed.
    state.idleCycles += 1;
    if (state.idleCycles >= IDLE_THRESHOLD_CYCLES) {
      state.currentIntervalMs = Math.min(
        state.currentIntervalMs * IDLE_MULTIPLIER,
        MAX_POLL_INTERVAL_MS,
      );
    }
  } else {
    // Active network → pull the wait delay back toward the minimum.
    state.idleCycles = 0;
    state.currentIntervalMs = Math.max(
      MIN_POLL_INTERVAL_MS,
      Math.floor(state.currentIntervalMs * LOAD_DECREASE_FACTOR),
    );
  }

  state.lastLoadAdjustmentAt = Date.now();

  logger.debug("indexer_runner throttle adjustment", {
    processedEventCount,
    currentIntervalMs: state.currentIntervalMs,
    idleCycles: state.idleCycles,
  });

  return { ...state };
}
