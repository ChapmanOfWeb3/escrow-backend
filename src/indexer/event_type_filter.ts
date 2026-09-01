import type { Server } from "@stellar/stellar-sdk/rpc";
import logger from "../utils/logger.js";

/**
 * The set of Soroban contract event types the indexer cares about.
 * Only events whose first topic matches one of these strings will be fetched.
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

/** Starting delay (ms) applied after the first failed attempt. */
export const DEFAULT_BACKOFF_MS = 1_000;

/** Hard ceiling so exponential growth never exceeds five minutes. */
export const MAX_BACKOFF_MS = 300_000;

/** Multiplier applied to the current backoff on every consecutive failure. */
export const BACKOFF_MULTIPLIER = 2;

/** Maximum number of attempts (1 initial + N retries) before giving up. */
export const MAX_ATTEMPTS = 5;

/** Errors whose messages indicate an RPC transport / connection problem. */
const CONNECTION_ERROR_PATTERNS = [
  /ECONNREFUSED/i,
  /ECONNRESET/i,
  /ETIMEDOUT/i,
  /ENOTFOUND/i,
  /EHOSTUNREACH/i,
  /network.*error/i,
  /connection.*timeout/i,
  /socket.*hang.*up/i,
  /fetch.*failed/i,
  /request.*timeout/i,
  /getaddrinfo/i,
  /connect.*ECONNREFUSED/i,
] as const;

/**
 * Determine whether `err` represents a transient RPC connection / network
 * failure that should trigger a retry rather than an immediate abort.
 */
export function isConnectionError(err: unknown): boolean {
  if (!(err instanceof Error)) return false;
  const msg = err.message;
  return CONNECTION_ERROR_PATTERNS.some((pattern) => pattern.test(msg));
}

/**
 * Compute the next backoff duration using exponential growth capped at
 * `MAX_BACKOFF_MS`.
 *
 * @param currentBackoffMs  The backoff used for the most-recent delay.
 * @returns                 The next backoff in milliseconds.
 */
export function nextBackoff(currentBackoffMs: number): number {
  return Math.min(currentBackoffMs * BACKOFF_MULTIPLIER, MAX_BACKOFF_MS);
}

/** Parameters forwarded directly to `Server.getEvents`. */
export interface GetEventsParams {
  startLedger: number;
  contractIds: string[];
  limit?: number;
}

/** Shape of `Server.getEvents` that this module depends on (subset). */
export interface RpcGetEventsResult {
  events: Array<{
    contractId: { contractId(): string } | undefined;
    topic: unknown[];
    value: unknown;
    ledger: number;
    ledgerClosedAt: string | undefined;
  }>;
}

/**
 * Options for `fetchEventsWithRetry`.
 */
export interface FetchEventsOptions {
  /** Override the default maximum attempts (default: MAX_ATTEMPTS). */
  maxAttempts?: number;
  /** Override the initial backoff delay in ms (default: DEFAULT_BACKOFF_MS). */
  initialBackoffMs?: number;
  /**
   * Inject a custom sleep function so tests can skip real delays.
   * Defaults to a Promise-based setTimeout wrapper.
   */
  sleep?: (ms: number) => Promise<void>;
}

const defaultSleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Fetch filtered contract events from the Soroban RPC with exponential-backoff
 * retry on transient connection errors.
 *
 * Only the event types listed in `EVENT_TYPES` are requested, so irrelevant
 * contract events are never returned to the caller.
 *
 * On each connection failure the wait before the next attempt doubles, starting
 * at `initialBackoffMs` and never exceeding `MAX_BACKOFF_MS`.  Non-connection
 * errors (e.g. bad request, contract-not-found) are propagated immediately
 * without retrying.
 *
 * @param server   Soroban RPC `Server` instance (or compatible mock).
 * @param params   Event query parameters.
 * @param options  Optional overrides for retry tuning and sleep injection.
 * @returns        The raw `getEvents` response on success.
 * @throws         Re-throws the last error after `maxAttempts` are exhausted,
 *                 or the first non-connection error encountered.
 */
export async function fetchEventsWithRetry(
  server: Pick<Server, "getEvents">,
  params: GetEventsParams,
  options: FetchEventsOptions = {}
): Promise<RpcGetEventsResult> {
  const maxAttempts = options.maxAttempts ?? MAX_ATTEMPTS;
  const sleep = options.sleep ?? defaultSleep;

  let backoffMs = options.initialBackoffMs ?? DEFAULT_BACKOFF_MS;
  let lastError: unknown;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const result = await server.getEvents({
        startLedger: params.startLedger,
        filters: [
          {
            type: "contract",
            contractIds: params.contractIds,
            topics: [[...EVENT_TYPES]],
          },
        ],
        limit: params.limit ?? 100,
      });

      if (attempt > 1) {
        logger.info("event_type_filter: RPC getEvents succeeded after retry", {
          attempt,
          startLedger: params.startLedger,
        });
      }

      return result as unknown as RpcGetEventsResult;
    } catch (err) {
      lastError = err;

      // Non-connection errors are unrecoverable – propagate immediately.
      if (!isConnectionError(err)) {
        logger.error("event_type_filter: non-connection error from getEvents", {
          attempt,
          error: err instanceof Error ? err.message : String(err),
        });
        throw err;
      }

      const attemptsLeft = maxAttempts - attempt;

      if (attemptsLeft === 0) {
        logger.error(
          "event_type_filter: all retry attempts exhausted for getEvents",
          {
            maxAttempts,
            startLedger: params.startLedger,
            error: err instanceof Error ? err.message : String(err),
          }
        );
        break;
      }

      logger.warn(
        "event_type_filter: RPC connection error – retrying with backoff",
        {
          attempt,
          attemptsLeft,
          backoffMs,
          error: err instanceof Error ? err.message : String(err),
        }
      );

      await sleep(backoffMs);
      backoffMs = nextBackoff(backoffMs);
    }
  }

  throw lastError;
}
