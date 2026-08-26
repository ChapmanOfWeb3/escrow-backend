import Database from "better-sqlite3";
import { setDb, runMigrations, insertEvent, getLastIndexedLedger } from "../src/indexer/db.js";
import {
  initializeSyncRangesTable,
  isLedgerSynced,
  getSyncedRanges,
  findUnsyncedRanges,
  insertEventsWithDedup,
  countEventsInRange,
  deleteEventsInRange,
  type SyncRange,
} from "../src/indexer/duplicate-prevention.js";

describe("DuplicatePrevention – dynamic historical sync ranges", () => {
  let testDb: Database.Database;

  beforeAll(() => {
    testDb = new Database(":memory:");
    setDb(testDb);
  });

  afterAll(() => {
    testDb.close();
  });

  beforeEach(() => {
    testDb.exec("DROP TABLE IF EXISTS events");
    testDb.exec("DROP TABLE IF EXISTS indexer_state");
    testDb.exec("DROP TABLE IF EXISTS monitored_contracts");
    testDb.exec("DROP TABLE IF EXISTS schema_migrations");
    testDb.exec("DROP TABLE IF EXISTS webhook_subscriptions");
    testDb.exec("DROP TABLE IF EXISTS sync_ranges");
    runMigrations();
  });

  describe("initializeSyncRangesTable", () => {
    it("creates sync_ranges table", () => {
      initializeSyncRangesTable();
      const row = testDb
        .prepare(
          "SELECT name FROM sqlite_master WHERE type='table' AND name='sync_ranges'"
        )
        .get();
      expect(row).toBeTruthy();
    });

    it("is idempotent", () => {
      initializeSyncRangesTable();
      expect(() => initializeSyncRangesTable()).not.toThrow();
    });
  });

  describe("isLedgerSynced", () => {
    it("returns false when no ranges are synced", () => {
      initializeSyncRangesTable();
      expect(isLedgerSynced(500)).toBe(false);
    });

    it("returns true for a ledger within a synced range", () => {
      initializeSyncRangesTable();
      insertEventsWithDedup([], { startLedger: 100, endLedger: 200 });
      expect(isLedgerSynced(150)).toBe(true);
    });

    it("returns false for a ledger outside synced ranges", () => {
      initializeSyncRangesTable();
      insertEventsWithDedup([], { startLedger: 100, endLedger: 200 });
      expect(isLedgerSynced(250)).toBe(false);
    });

    it("returns true at exact boundaries", () => {
      initializeSyncRangesTable();
      insertEventsWithDedup([], { startLedger: 100, endLedger: 200 });
      expect(isLedgerSynced(100)).toBe(true);
      expect(isLedgerSynced(200)).toBe(true);
    });
  });

  describe("getSyncedRanges", () => {
    it("returns empty array when nothing is synced", () => {
      initializeSyncRangesTable();
      expect(getSyncedRanges()).toEqual([]);
    });

    it("returns ranges sorted by start_ledger", () => {
      initializeSyncRangesTable();
      insertEventsWithDedup([], { startLedger: 300, endLedger: 400 });
      insertEventsWithDedup([], { startLedger: 100, endLedger: 200 });
      const ranges = getSyncedRanges();
      expect(ranges.length).toBe(2);
      expect(ranges[0].startLedger).toBe(100);
      expect(ranges[1].startLedger).toBe(300);
    });
  });

  describe("findUnsyncedRanges", () => {
    it("returns full range when nothing is synced", () => {
      initializeSyncRangesTable();
      const gaps = findUnsyncedRanges(100, 500);
      expect(gaps).toEqual([{ startLedger: 100, endLedger: 500 }]);
    });

    it("returns gap before first synced range", () => {
      initializeSyncRangesTable();
      insertEventsWithDedup([], { startLedger: 300, endLedger: 500 });
      const gaps = findUnsyncedRanges(100, 600);
      expect(gaps).toEqual([
        { startLedger: 100, endLedger: 299 },
        { startLedger: 501, endLedger: 600 },
      ]);
    });

    it("returns gap between two synced ranges", () => {
      initializeSyncRangesTable();
      insertEventsWithDedup([], { startLedger: 100, endLedger: 200 });
      insertEventsWithDedup([], { startLedger: 400, endLedger: 500 });
      const gaps = findUnsyncedRanges(100, 600);
      expect(gaps).toEqual([
        { startLedger: 201, endLedger: 399 },
        { startLedger: 501, endLedger: 600 },
      ]);
    });

    it("returns empty array when fully covered", () => {
      initializeSyncRangesTable();
      insertEventsWithDedup([], { startLedger: 100, endLedger: 600 });
      const gaps = findUnsyncedRanges(100, 600);
      expect(gaps).toEqual([]);
    });
  });

  describe("insertEventsWithDedup", () => {
    it("inserts new events and returns correct counts", () => {
      initializeSyncRangesTable();
      const events = [
        {
          contractId: "C1",
          eventType: "initialized",
          ledgerSequence: 100,
          timestamp: 1000,
          dataJson: "{}",
        },
        {
          contractId: "C1",
          eventType: "funded",
          ledgerSequence: 101,
          timestamp: 1001,
          dataJson: "{}",
        },
      ];

      const result = insertEventsWithDedup(events, {
        startLedger: 100,
        endLedger: 101,
      });

      expect(result.totalProcessed).toBe(2);
      expect(result.newEventsInserted).toBe(2);
      expect(result.duplicatesFound).toBe(0);
    });

    it("skips duplicate events on re-sync", () => {
      initializeSyncRangesTable();
      const events = [
        {
          contractId: "C1",
          eventType: "initialized",
          ledgerSequence: 100,
          timestamp: 1000,
          dataJson: "{}",
        },
      ];

      insertEventsWithDedup(events, { startLedger: 100, endLedger: 100 });

      const result = insertEventsWithDedup(events, {
        startLedger: 100,
        endLedger: 100,
      });

      expect(result.totalProcessed).toBe(1);
      expect(result.newEventsInserted).toBe(0);
      expect(result.duplicatesFound).toBe(1);
    });

    it("counts block event counts correctly across contracts", () => {
      initializeSyncRangesTable();
      const events = [
        {
          contractId: "C1",
          eventType: "initialized",
          ledgerSequence: 100,
          timestamp: 1000,
          dataJson: "{}",
        },
        {
          contractId: "C2",
          eventType: "initialized",
          ledgerSequence: 100,
          timestamp: 1000,
          dataJson: "{}",
        },
        {
          contractId: "C3",
          eventType: "funded",
          ledgerSequence: 101,
          timestamp: 1001,
          dataJson: "{}",
        },
      ];

      const result = insertEventsWithDedup(events, {
        startLedger: 100,
        endLedger: 101,
      });

      expect(result.newEventsInserted).toBe(3);
      expect(countEventsInRange(100, 101)).toBe(3);
    });

    it("handles empty event arrays", () => {
      initializeSyncRangesTable();
      const result = insertEventsWithDedup([], {
        startLedger: 500,
        endLedger: 600,
      });

      expect(result.totalProcessed).toBe(0);
      expect(result.newEventsInserted).toBe(0);
      expect(result.duplicatesFound).toBe(0);
    });

    it("tracks sync range metadata", () => {
      initializeSyncRangesTable();
      insertEventsWithDedup(
        [
          {
            contractId: "C1",
            eventType: "initialized",
            ledgerSequence: 100,
            timestamp: 1000,
            dataJson: "{}",
          },
        ],
        { startLedger: 100, endLedger: 150 }
      );

      const ranges = getSyncedRanges();
      expect(ranges.length).toBe(1);
      expect(ranges[0].startLedger).toBe(100);
      expect(ranges[0].endLedger).toBe(150);
      expect(ranges[0].eventCount).toBe(1);
      expect(ranges[0].duplicateCount).toBe(0);
    });
  });

  describe("countEventsInRange", () => {
    it("returns 0 for empty range", () => {
      expect(countEventsInRange(100, 200)).toBe(0);
    });

    it("counts events in range correctly", () => {
      insertEvent("C1", "initialized", 100, 1000, "{}");
      insertEvent("C1", "funded", 101, 1001, "{}");
      insertEvent("C1", "delivered", 102, 1002, "{}");

      expect(countEventsInRange(100, 101)).toBe(2);
      expect(countEventsInRange(100, 102)).toBe(3);
      expect(countEventsInRange(101, 102)).toBe(2);
      expect(countEventsInRange(200, 300)).toBe(0);
    });
  });

  describe("deleteEventsInRange", () => {
    it("deletes events in range", () => {
      insertEvent("C1", "initialized", 100, 1000, "{}");
      insertEvent("C1", "funded", 101, 1001, "{}");
      insertEvent("C1", "delivered", 102, 1002, "{}");

      const deleted = deleteEventsInRange(100, 101);
      expect(deleted).toBe(2);
      expect(countEventsInRange(100, 102)).toBe(1);
    });

    it("cleans up sync_ranges metadata for deleted range", () => {
      initializeSyncRangesTable();
      insertEventsWithDedup(
        [
          {
            contractId: "C1",
            eventType: "initialized",
            ledgerSequence: 100,
            timestamp: 1000,
            dataJson: "{}",
          },
        ],
        { startLedger: 100, endLedger: 200 }
      );

      deleteEventsInRange(100, 200);
      const ranges = getSyncedRanges();
      expect(ranges.length).toBe(0);
    });
  });
});
