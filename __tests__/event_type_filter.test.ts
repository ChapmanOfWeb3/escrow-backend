import { jest } from "@jest/globals";
import Database from "better-sqlite3";
import { setDb, runMigrations } from "../src/indexer/db.js";

const mockLogger = {
  info: jest.fn<(...args: unknown[]) => void>(),
  warn: jest.fn<(...args: unknown[]) => void>(),
  error: jest.fn<(...args: unknown[]) => void>(),
  debug: jest.fn<(...args: unknown[]) => void>(),
};

jest.unstable_mockModule("../src/utils/logger.js", () => ({
  default: mockLogger,
}));

const {
  EVENT_TYPES,
  CONSECUTIVE_ERROR_THRESHOLD,
  isKnownEventType,
  parseEventTopic,
  filterMatchingEvents,
  eventLockKey,
  withEventQueueLock,
  enqueueEventInsert,
  ingestRpcEvents,
  getEventQueueDepth,
  resetEventQueueForTests,
  recordFilterSuccess,
  recordFilterError,
  checkFilterStall,
  getEventFilterHealth,
  resetEventFilterHealth,
  adjustFilterPollInterval,
  getFilterPollState,
  getCurrentFilterIntervalMs,
  resetFilterPollState,
} = await import("../src/indexer/event_type_filter.js");

const CONTRACT = "CCONTRACT000000000000000000000000000000000000000000000";

function notification(overrides: Partial<{
  contractId: string;
  topic: unknown[];
  ledger: number;
  timestamp: number;
  value: unknown;
}> = {}) {
  return {
    contractId: CONTRACT,
    topic: ["funded"],
    ledger: 100,
    timestamp: 1_700_000_000,
    value: { amount: 1000 },
    ...overrides,
  };
}

function countEvents(db: Database.Database): number {
  const row = db.prepare("SELECT COUNT(*) AS n FROM events").get() as {
    n: number;
  };
  return row.n;
}

describe("event_type_filter", () => {
  let testDb: Database.Database;

  beforeEach(() => {
    testDb = new Database(":memory:");
    setDb(testDb);
    runMigrations();

    resetEventQueueForTests();
    resetEventFilterHealth();
    resetFilterPollState();
    jest.clearAllMocks();
  });

  afterEach(() => {
    testDb.close();
  });

  // =========================================================================
  // Topic parsing
  // =========================================================================

  describe("topic parsing", () => {
    it("recognises every canonical event type", () => {
      for (const type of EVENT_TYPES) {
        expect(isKnownEventType(type)).toBe(true);
      }
    });

    it("rejects an unknown topic string", () => {
      expect(isKnownEventType("not_a_real_event")).toBe(false);
    });

    it("rejects non-string topic values", () => {
      expect(isKnownEventType(42)).toBe(false);
      expect(isKnownEventType(null)).toBe(false);
      expect(isKnownEventType(undefined)).toBe(false);
      expect(isKnownEventType({})).toBe(false);
    });

    it("parses the event type from the first topic entry", () => {
      expect(parseEventTopic(["approved", "extra"])).toBe("approved");
    });

    it("returns null for an empty topic array", () => {
      expect(parseEventTopic([])).toBeNull();
    });

    it("returns null for a non-array topic", () => {
      expect(parseEventTopic("funded")).toBeNull();
      expect(parseEventTopic(null)).toBeNull();
      expect(parseEventTopic(undefined)).toBeNull();
    });

    it("returns null when the head is not a known type", () => {
      expect(parseEventTopic(["transfer"])).toBeNull();
    });
  });

  // =========================================================================
  // Batch filtering
  // =========================================================================

  describe("filterMatchingEvents", () => {
    it("matches a well-formed notification", () => {
      const { matched, rejected } = filterMatchingEvents([notification()]);
      expect(rejected).toHaveLength(0);
      expect(matched).toHaveLength(1);
      expect(matched[0]).toMatchObject({
        contractId: CONTRACT,
        eventType: "funded",
        ledgerSequence: 100,
        timestamp: 1_700_000_000,
      });
    });

    it("serialises the event value into dataJson", () => {
      const { matched } = filterMatchingEvents([
        notification({ value: { amount: 55 } }),
      ]);
      expect(JSON.parse(matched[0].dataJson)).toEqual({ amount: 55 });
    });

    it("defaults the timestamp when the notification omits it", () => {
      const { matched } = filterMatchingEvents([
        { contractId: CONTRACT, topic: ["funded"], ledger: 1, value: {} },
      ]);
      expect(matched[0].timestamp).toBeGreaterThan(0);
    });

    it("rejects an unmatched topic with a reason", () => {
      const { matched, rejected } = filterMatchingEvents([
        notification({ topic: ["transfer"] }),
      ]);
      expect(matched).toHaveLength(0);
      expect(rejected[0].reason).toBe("unmatched event topic");
    });

    it("rejects a notification with no contract id", () => {
      const { rejected } = filterMatchingEvents([
        notification({ contractId: "" }),
      ]);
      expect(rejected[0].reason).toBe("missing contractId");
    });

    it("rejects a non-integer or negative ledger", () => {
      expect(
        filterMatchingEvents([notification({ ledger: -1 })])[
          "rejected"
        ][0].reason,
      ).toBe("invalid ledger sequence");
      expect(
        filterMatchingEvents([notification({ ledger: 1.5 })])[
          "rejected"
        ][0].reason,
      ).toBe("invalid ledger sequence");
    });

    it("keeps good events when a bad one is in the same batch", () => {
      const { matched, rejected } = filterMatchingEvents([
        notification({ ledger: 1 }),
        notification({ topic: ["garbage"], ledger: 2 }),
        notification({ ledger: 3, topic: ["approved"] }),
      ]);
      expect(matched).toHaveLength(2);
      expect(rejected).toHaveLength(1);
    });

    it("never throws on a malformed batch", () => {
      expect(() =>
        filterMatchingEvents([
          notification({ topic: undefined }),
          notification({ contractId: undefined as unknown as string }),
        ]),
      ).not.toThrow();
    });

    it("returns empty results for an empty batch", () => {
      expect(filterMatchingEvents([])).toEqual({ matched: [], rejected: [] });
    });
  });

  // =========================================================================
  // Issue 1 — threshold warning alerts
  // =========================================================================

  describe("threshold warning alerts", () => {
    it("warns below the threshold without alerting", () => {
      recordFilterError(new Error("rpc down"));
      expect(mockLogger.warn).toHaveBeenCalled();
      expect(mockLogger.error).not.toHaveBeenCalled();
      expect(getEventFilterHealth().alerting).toBe(false);
    });

    it("escalates to an error alert once the threshold is reached", () => {
      for (let i = 0; i < CONSECUTIVE_ERROR_THRESHOLD; i++) {
        recordFilterError(new Error(`failure ${i}`));
      }

      expect(mockLogger.error).toHaveBeenCalledWith(
        "Event filter alert: consecutive error threshold reached",
        expect.objectContaining({
          consecutiveErrors: CONSECUTIVE_ERROR_THRESHOLD,
          threshold: CONSECUTIVE_ERROR_THRESHOLD,
        }),
      );
      expect(getEventFilterHealth().alerting).toBe(true);
    });

    it("does not alert one failure short of the threshold", () => {
      for (let i = 0; i < CONSECUTIVE_ERROR_THRESHOLD - 1; i++) {
        recordFilterError(new Error("nope"));
      }
      expect(mockLogger.error).not.toHaveBeenCalled();
    });

    it("keeps alerting while the condition persists", () => {
      for (let i = 0; i < CONSECUTIVE_ERROR_THRESHOLD + 2; i++) {
        recordFilterError(new Error("still broken"));
      }
      expect(mockLogger.error.mock.calls.length).toBe(3);
    });

    it("counts the errors it has seen", () => {
      recordFilterError(new Error("a"));
      recordFilterError(new Error("b"));
      expect(getEventFilterHealth().consecutiveErrors).toBe(2);
    });

    it("records the most recent error message", () => {
      recordFilterError(new Error("first"));
      recordFilterError(new Error("second"));
      expect(getEventFilterHealth().lastError).toBe("second");
    });

    it("stringifies a non-Error failure", () => {
      recordFilterError("plain string failure");
      expect(getEventFilterHealth().lastError).toBe("plain string failure");
    });

    it("resets the counter on success", () => {
      recordFilterError(new Error("blip"));
      recordFilterError(new Error("blip"));
      recordFilterSuccess(3);

      const state = getEventFilterHealth();
      expect(state.consecutiveErrors).toBe(0);
      expect(state.lastError).toBeNull();
      expect(state.lastSuccessAt).not.toBeNull();
    });

    it("clears an active alert on recovery and says so", () => {
      for (let i = 0; i < CONSECUTIVE_ERROR_THRESHOLD; i++) {
        recordFilterError(new Error("down"));
      }
      recordFilterSuccess(1);

      expect(getEventFilterHealth().alerting).toBe(false);
      expect(mockLogger.info).toHaveBeenCalledWith(
        "Event filter recovered – clearing alert state",
        expect.objectContaining({ matchedCount: 1 }),
      );
    });

    it("needs a fresh run of failures to re-alert after recovery", () => {
      for (let i = 0; i < CONSECUTIVE_ERROR_THRESHOLD; i++) {
        recordFilterError(new Error("down"));
      }
      recordFilterSuccess();
      mockLogger.error.mockClear();

      recordFilterError(new Error("down again"));
      expect(mockLogger.error).not.toHaveBeenCalled();
    });

    it("reports a stall once the window has elapsed", () => {
      recordFilterSuccess();
      const future = Date.now() + 120_001;

      expect(checkFilterStall(future)).toBe(true);
      expect(mockLogger.error).toHaveBeenCalledWith(
        "Event filter alert: processing stalled",
        expect.objectContaining({ stallThresholdMs: 120_000 }),
      );
    });

    it("does not report a stall inside the window", () => {
      recordFilterSuccess();
      expect(checkFilterStall(Date.now() + 1_000)).toBe(false);
      expect(mockLogger.error).not.toHaveBeenCalled();
    });

    it("does not report a stall before the first success", () => {
      expect(checkFilterStall(Date.now() + 10_000_000)).toBe(false);
    });

    it("resetEventFilterHealth clears everything", () => {
      recordFilterError(new Error("x"));
      resetEventFilterHealth();

      expect(getEventFilterHealth()).toEqual({
        consecutiveErrors: 0,
        lastSuccessAt: null,
        lastError: null,
        alerting: false,
      });
    });
  });

  // =========================================================================
  // Issue 2 — simulated RPC events reach the database schema
  // =========================================================================

  describe("simulated RPC event ingestion", () => {
    it("writes a simulated event into the events table", async () => {
      const result = await ingestRpcEvents([notification()]);

      expect(result.inserted).toBe(1);
      expect(countEvents(testDb)).toBe(1);
    });

    it("persists every column of the schema", async () => {
      await ingestRpcEvents([
        notification({ ledger: 42, value: { milestone: 2 } }),
      ]);

      const row = testDb
        .prepare("SELECT * FROM events WHERE ledger_sequence = 42")
        .get() as Record<string, unknown>;

      expect(row.contract_id).toBe(CONTRACT);
      expect(row.event_type).toBe("funded");
      expect(row.ledger_sequence).toBe(42);
      expect(row.timestamp).toBe(1_700_000_000);
      expect(JSON.parse(row.data_json as string)).toEqual({ milestone: 2 });
    });

    it("writes one row per canonical event type", async () => {
      const batch = EVENT_TYPES.map((type, idx) =>
        notification({ topic: [type], ledger: 200 + idx }),
      );

      const result = await ingestRpcEvents(batch);

      expect(result.inserted).toBe(EVENT_TYPES.length);
      expect(countEvents(testDb)).toBe(EVENT_TYPES.length);

      const stored = testDb
        .prepare("SELECT event_type FROM events ORDER BY ledger_sequence")
        .all() as Array<{ event_type: string }>;
      expect(stored.map((r) => r.event_type)).toEqual([...EVENT_TYPES]);
    });

    it("writes a multi-ledger simulated stream in order", async () => {
      const batch = Array.from({ length: 25 }, (_, i) =>
        notification({ ledger: 1_000 + i }),
      );

      await ingestRpcEvents(batch);

      const rows = testDb
        .prepare("SELECT ledger_sequence FROM events ORDER BY ledger_sequence")
        .all() as Array<{ ledger_sequence: number }>;

      expect(rows).toHaveLength(25);
      expect(rows[0].ledger_sequence).toBe(1_000);
      expect(rows[24].ledger_sequence).toBe(1_024);
    });

    it("does not write events the filter rejected", async () => {
      const result = await ingestRpcEvents([
        notification({ ledger: 1 }),
        notification({ ledger: 2, topic: ["transfer"] }),
      ]);

      expect(result.inserted).toBe(1);
      expect(result.rejected).toBe(1);
      expect(countEvents(testDb)).toBe(1);
    });

    it("reports duplicates separately from inserts", async () => {
      await ingestRpcEvents([notification()]);
      const second = await ingestRpcEvents([notification()]);

      expect(second.inserted).toBe(0);
      expect(second.duplicates).toBe(1);
      expect(countEvents(testDb)).toBe(1);
    });

    it("separates events from different contracts at the same ledger", async () => {
      await ingestRpcEvents([
        notification({ contractId: "CONTRACT_A" }),
        notification({ contractId: "CONTRACT_B" }),
      ]);
      expect(countEvents(testDb)).toBe(2);
    });

    it("separates different event types at the same ledger", async () => {
      await ingestRpcEvents([
        notification({ topic: ["funded"] }),
        notification({ topic: ["approved"] }),
      ]);
      expect(countEvents(testDb)).toBe(2);
    });

    it("records a successful pass in filter health", async () => {
      await ingestRpcEvents([notification()]);
      expect(getEventFilterHealth().lastSuccessAt).not.toBeNull();
    });

    it("handles an empty batch without touching the database", async () => {
      const result = await ingestRpcEvents([]);
      expect(result).toEqual({ inserted: 0, duplicates: 0, rejected: 0 });
      expect(countEvents(testDb)).toBe(0);
    });

    it("records an error and rethrows when the insert fails", async () => {
      testDb.close();

      await expect(ingestRpcEvents([notification()])).rejects.toThrow();
      expect(getEventFilterHealth().consecutiveErrors).toBe(1);

      // Restore a live handle so afterEach can close cleanly.
      testDb = new Database(":memory:");
      setDb(testDb);
    });
  });

  // =========================================================================
  // Issue 3 — memory queue locks
  // =========================================================================

  describe("memory queue locks", () => {
    it("keys the lock by the unique constraint columns", () => {
      const key = eventLockKey({
        contractId: "C1",
        eventType: "funded",
        ledgerSequence: 7,
        timestamp: 0,
        dataJson: "{}",
      });
      expect(key).toBe("C1|7|funded");
    });

    it("serialises concurrent callers on the same key", async () => {
      const order: string[] = [];

      await Promise.all([
        withEventQueueLock("k", async () => {
          order.push("first:start");
          await new Promise((r) => setTimeout(r, 20));
          order.push("first:end");
        }),
        withEventQueueLock("k", async () => {
          order.push("second:start");
          order.push("second:end");
        }),
      ]);

      expect(order).toEqual([
        "first:start",
        "first:end",
        "second:start",
        "second:end",
      ]);
    });

    it("allows different keys to run concurrently", async () => {
      const order: string[] = [];

      await Promise.all([
        withEventQueueLock("a", async () => {
          order.push("a:start");
          await new Promise((r) => setTimeout(r, 20));
          order.push("a:end");
        }),
        withEventQueueLock("b", async () => {
          order.push("b:start");
        }),
      ]);

      // b does not wait for a to finish.
      expect(order.indexOf("b:start")).toBeLessThan(order.indexOf("a:end"));
    });

    it("does not deadlock a key after a failure", async () => {
      await expect(
        withEventQueueLock("k", async () => {
          throw new Error("boom");
        }),
      ).rejects.toThrow("boom");

      await expect(withEventQueueLock("k", async () => "ok")).resolves.toBe(
        "ok",
      );
    });

    it("propagates the callback result", async () => {
      await expect(withEventQueueLock("k", () => 42)).resolves.toBe(42);
    });

    it("drains the queue once contention clears", async () => {
      await Promise.all([
        withEventQueueLock("k", async () => {}),
        withEventQueueLock("k", async () => {}),
      ]);
      expect(getEventQueueDepth()).toBe(0);
    });

    it("does not duplicate rows under concurrent identical inserts", async () => {
      const event = {
        contractId: CONTRACT,
        eventType: "funded",
        ledgerSequence: 500,
        timestamp: 1,
        dataJson: "{}",
      };

      const results = await Promise.all(
        Array.from({ length: 10 }, () => enqueueEventInsert({ ...event })),
      );

      expect(results.filter(Boolean)).toHaveLength(1);
      expect(countEvents(testDb)).toBe(1);
    });

    it("does not duplicate rows across concurrent overlapping batches", async () => {
      const batch = [
        notification({ ledger: 900 }),
        notification({ ledger: 901 }),
      ];

      await Promise.all([
        ingestRpcEvents(batch.map((n) => ({ ...n }))),
        ingestRpcEvents(batch.map((n) => ({ ...n }))),
        ingestRpcEvents(batch.map((n) => ({ ...n }))),
      ]);

      expect(countEvents(testDb)).toBe(2);
    });

    it("inserts exactly once across concurrent notification storms", async () => {
      const storm = Array.from({ length: 20 }, () =>
        ingestRpcEvents([notification({ ledger: 777 })]),
      );

      const results = await Promise.all(storm);
      const totalInserted = results.reduce((sum, r) => sum + r.inserted, 0);

      expect(totalInserted).toBe(1);
      expect(countEvents(testDb)).toBe(1);
    });

    it("keeps distinct concurrent events from blocking each other", async () => {
      await Promise.all(
        Array.from({ length: 15 }, (_, i) =>
          ingestRpcEvents([notification({ ledger: 2_000 + i })]),
        ),
      );
      expect(countEvents(testDb)).toBe(15);
    });
  });

  // =========================================================================
  // Issue 4 — dynamic polling frequency
  // =========================================================================

  describe("dynamic polling intervals", () => {
    it("starts at the base interval", () => {
      expect(getCurrentFilterIntervalMs()).toBe(15_000);
    });

    it("holds steady for the first idle cycle", () => {
      const state = adjustFilterPollInterval(0);
      expect(state.currentIntervalMs).toBe(15_000);
      expect(state.idleCycles).toBe(1);
    });

    it("increases the delay once the network stays idle", () => {
      adjustFilterPollInterval(0);
      const state = adjustFilterPollInterval(0);

      expect(state.currentIntervalMs).toBe(30_000);
      expect(state.idleCycles).toBe(2);
    });

    it("keeps backing off across further idle cycles", () => {
      const seen: number[] = [];
      for (let i = 0; i < 5; i++) {
        seen.push(adjustFilterPollInterval(0).currentIntervalMs);
      }
      // Monotonically non-decreasing while idle.
      for (let i = 1; i < seen.length; i++) {
        expect(seen[i]).toBeGreaterThanOrEqual(seen[i - 1]);
      }
      expect(seen[seen.length - 1]).toBeGreaterThan(seen[0]);
    });

    it("caps the delay at the maximum", () => {
      for (let i = 0; i < 20; i++) adjustFilterPollInterval(0);
      expect(getCurrentFilterIntervalMs()).toBe(60_000);
    });

    it("shortens the delay under load", () => {
      for (let i = 0; i < 5; i++) adjustFilterPollInterval(0);
      const backedOff = getCurrentFilterIntervalMs();

      const state = adjustFilterPollInterval(10);
      expect(state.currentIntervalMs).toBeLessThan(backedOff);
    });

    it("floors the delay at the minimum under sustained load", () => {
      for (let i = 0; i < 20; i++) adjustFilterPollInterval(50);
      expect(getCurrentFilterIntervalMs()).toBe(5_000);
    });

    it("resets the idle counter as soon as events arrive", () => {
      adjustFilterPollInterval(0);
      adjustFilterPollInterval(0);
      expect(getFilterPollState().idleCycles).toBe(2);

      adjustFilterPollInterval(1);
      expect(getFilterPollState().idleCycles).toBe(0);
    });

    it("treats a negative count as idle", () => {
      adjustFilterPollInterval(-5);
      expect(getFilterPollState().idleCycles).toBe(1);
    });

    it("records the last matched count", () => {
      adjustFilterPollInterval(7);
      expect(getFilterPollState().lastMatchedCount).toBe(7);
    });

    it("stamps the adjustment time", () => {
      const before = Date.now();
      const state = adjustFilterPollInterval(1);
      expect(state.lastAdjustedAt).toBeGreaterThanOrEqual(before);
    });

    it("treats a payload of only rejected events as idle", () => {
      // A poll that returned events but matched none is idle for the indexer.
      const { matched } = filterMatchingEvents([
        notification({ topic: ["transfer"] }),
        notification({ topic: ["mint"] }),
      ]);

      adjustFilterPollInterval(matched.length);
      const state = adjustFilterPollInterval(matched.length);

      expect(state.idleCycles).toBe(2);
      expect(state.currentIntervalMs).toBe(30_000);
    });

    it("returns a snapshot rather than a live reference", () => {
      const snapshot = getFilterPollState();
      adjustFilterPollInterval(5);
      expect(snapshot.lastMatchedCount).toBe(0);
    });

    it("resetFilterPollState returns to the base interval", () => {
      for (let i = 0; i < 5; i++) adjustFilterPollInterval(0);
      resetFilterPollState();

      expect(getCurrentFilterIntervalMs()).toBe(15_000);
      expect(getFilterPollState().idleCycles).toBe(0);
    });
  });
});
