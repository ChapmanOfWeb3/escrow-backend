import logger from "../utils/logger.js";

/**
 * Indexer runner failure / stall alerting (#253).
 *
 * Tracks consecutive poll failures and emits threshold warnings so operators
 * can react when the indexer event poller stalls or fails repeatedly.
 */

const DEFAULT_FAILURE_THRESHOLD = 3;
const DEFAULT_STALL_THRESHOLD_MS = 120_000;

export type IndexerRunnerFailureType =
  | "poll"
  | "rpc"
  | "persist"
  | "stall";

function readPositiveIntEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return fallback;
  const n = parseInt(raw, 10);
  if (!Number.isInteger(n) || n < 1) return fallback;
  return n;
}

export class IndexerRunnerFailureMonitor {
  readonly name: string;
  readonly failureThreshold: number;
  readonly stallThresholdMs: number;
  private consecutiveFailures = 0;
  private lastSuccessfulAt: number | null = null;
  private alertActive = false;

  constructor(
    options: {
      name?: string;
      failureThreshold?: number;
      stallThresholdMs?: number;
    } = {},
  ) {
    this.name = options.name ?? "indexer_runner";
    this.failureThreshold =
      options.failureThreshold ??
      readPositiveIntEnv(
        "INDEXER_RUNNER_FAILURE_THRESHOLD",
        readPositiveIntEnv("POLLER_FAILURE_THRESHOLD", DEFAULT_FAILURE_THRESHOLD),
      );
    this.stallThresholdMs =
      options.stallThresholdMs ??
      readPositiveIntEnv(
        "INDEXER_RUNNER_STALL_THRESHOLD_MS",
        readPositiveIntEnv("POLLER_STALL_THRESHOLD_MS", DEFAULT_STALL_THRESHOLD_MS),
      );
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

  getFailureThreshold(): number {
    return this.failureThreshold;
  }

  /**
   * Record a failure. Logs an error on every attempt and emits a warning alert
   * only when the consecutive-failure threshold is first reached (#253).
   */
  recordFailure(
    failureType: IndexerRunnerFailureType,
    details: { error?: string; elapsedMs?: number } = {},
  ): number {
    this.consecutiveFailures += 1;
    const payload = {
      runner: this.name,
      failureType,
      consecutiveFailures: this.consecutiveFailures,
      threshold: this.failureThreshold,
      error: details.error,
      elapsedMs: details.elapsedMs,
    };

    logger.error("indexer_runner operation failed", payload);

    if (this.consecutiveFailures === this.failureThreshold) {
      this.alertActive = true;
      logger.warn(
        "indexer_runner alert: consecutive failure threshold reached",
        {
          ...payload,
          action:
            "Inspect RPC connectivity and indexer writes; the runner resumes automatically after the next successful poll.",
        },
      );
    }

    return this.consecutiveFailures;
  }

  recordSuccess(): void {
    const hadFailures = this.consecutiveFailures > 0 || this.alertActive;
    this.consecutiveFailures = 0;
    this.lastSuccessfulAt = Date.now();
    if (hadFailures) {
      logger.info("indexer_runner recovered after consecutive failures", {
        runner: this.name,
      });
    }
    this.alertActive = false;
  }

  /**
   * Emit a stall warning when no successful poll has occurred within the
   * configured stall window. Does not increment the consecutive-failure counter.
   * Stall threshold is read from env on each check so tests can override it (#253).
   */
  checkStall(): boolean {
    if (this.lastSuccessfulAt === null) return false;
    const stallThresholdMs = readPositiveIntEnv(
      "INDEXER_RUNNER_STALL_THRESHOLD_MS",
      readPositiveIntEnv("POLLER_STALL_THRESHOLD_MS", this.stallThresholdMs),
    );
    const elapsedMs = Date.now() - this.lastSuccessfulAt;
    if (elapsedMs <= stallThresholdMs) return false;
    logger.warn("indexer_runner alert: poller stall threshold reached", {
      runner: this.name,
      failureType: "stall" as const,
      consecutiveFailures: this.consecutiveFailures,
      threshold: this.failureThreshold,
      stallThresholdMs,
      elapsedMs,
      action:
        "No successful indexer_runner poll within the stall window; inspect RPC polling and indexer health.",
    });
    // Legacy poller stall message kept for #271 compatibility
    logger.warn("Poller stall detected – no successful poll for threshold period", {
      elapsedMs,
      stallThresholdMs,
      consecutiveFailures: this.consecutiveFailures,
    });
    return true;
  }

  reset(): void {
    this.consecutiveFailures = 0;
    this.lastSuccessfulAt = null;
    this.alertActive = false;
  }
}

const defaultMonitor = new IndexerRunnerFailureMonitor();

export function getIndexerRunnerFailureMonitor(): IndexerRunnerFailureMonitor {
  return defaultMonitor;
}

export function resetIndexerRunnerFailureState(): void {
  defaultMonitor.reset();
}
