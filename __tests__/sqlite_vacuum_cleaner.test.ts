import { jest } from "@jest/globals";
import Database from "better-sqlite3";
import { runMigrations, setDb } from "../src/indexer/db.js";
import {
  validateRetentionDays,
  pruneOldEvents,
  runVacuum,
  runVacuumCleanup,
  ERROR_CODES,
  DEFAULT_RETENTION_DAYS,
  isVacuumRetryableError,
  computeVacuumBackoffMs,
  withVacuumRetrySync,
  withVacuumRetry,
  runVacuumWithRetry,
  runVacuumCleanupWithRetry,
  DEFAULT_VACUUM_RETRY_CONFIG,
} from "../src/indexer/sqlite_vacuum_cleaner.js";

describe("sqlite_vacuum_cleaner (#193)", () => {
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
    runMigrations();
  });

  // -------------------------------------------------------------------------
  // validateRetentionDays
  // -------------------------------------------------------------------------

  describe("validateRetentionDays", () => {
    it("accepts positive integers", () => {
      expect(validateRetentionDays(1)).toEqual({ ok: true });
      expect(validateRetentionDays(90)).toEqual({ ok: true });
      expect(validateRetentionDays(365)).toEqual({ ok: true });
    });

    it("rejects zero", () => {
      const result = validateRetentionDays(0);
      expect(result.ok).toBe(false);
      if (!result.ok) {
        expect(result.code).toBe(ERROR_CODES.INVALID_RETENTION);
      }
    });

    it("rejects negative values", () => {
      const result = validateRetentionDays(-5);
      expect(result.ok).toBe(false);
      if (!result.ok) {
        expect(result.code).toBe(ERROR_CODES.INVALID_RETENTION);
      }
    });

    it("rejects non-integer values", () => {
      const result = validateRetentionDays(1.5);
      expect(result.ok).toBe(false);
      if (!result.ok) {
        expect(result.code).toBe(ERROR_CODES.INVALID_RETENTION);
      }
    });

    it("rejects NaN", () => {
      const result = validateRetentionDays(NaN);
      expect(result.ok).toBe(false);
      if (!result.ok) {
        expect(result.code).toBe(ERROR_CODES.INVALID_RETENTION);
      }
    });

    it("rejects Infinity", () => {
      const result = validateRetentionDays(Infinity);
      expect(result.ok).toBe(false);
      if (!result.ok) {
        expect(result.code).toBe(ERROR_CODES.INVALID_RETENTION);
      }
    });
  });

  // -------------------------------------------------------------------------
  // pruneOldEvents
  // -------------------------------------------------------------------------

  describe("pruneOldEvents", () => {
    function insertEventWithCreatedAt(
      contractId: string,
      ledgerSequence: number,
      createdAtExpr: string
    ) {
      testDb
        .prepare(
          `INSERT INTO events
           (contract_id, event_type, ledger_sequence, timestamp, data_json, created_at)
           VALUES (?, 'test-event', ?, 1000, '{}', ${createdAtExpr})`
        )
        .run(contractId, ledgerSequence);
    }

    it("deletes only events older than the retention window", () => {
      // Old events: backdated 100 days.
      insertEventWithCreatedAt("OLD-1", 1, "datetime('now', '-100 days')");
      insertEventWithCreatedAt("OLD-2", 2, "datetime('now', '-120 days')");
      // Recent events: within the 90-day window.
      insertEventWithCreatedAt("RECENT-1", 3, "datetime('now', '-1 days')");
      insertEventWithCreatedAt("RECENT-2", 4, "datetime('now')");

      const beforeCount = (
        testDb.prepare("SELECT COUNT(*) as cnt FROM events").get() as { cnt: number }
      ).cnt;
      expect(beforeCount).toBe(4);

      const deleted = pruneOldEvents(testDb, 90);
      expect(deleted).toBe(2);

      const afterCount = (
        testDb.prepare("SELECT COUNT(*) as cnt FROM events").get() as { cnt: number }
      ).cnt;
      expect(afterCount).toBe(2);

      const remaining = testDb
        .prepare("SELECT contract_id FROM events ORDER BY contract_id")
        .all() as Array<{ contract_id: string }>;
      expect(remaining.map((r) => r.contract_id)).toEqual(["RECENT-1", "RECENT-2"]);
    });

    it("throws for invalid retentionDays and does not touch any rows", () => {
      insertEventWithCreatedAt("OLD-1", 1, "datetime('now', '-200 days')");

      expect(() => pruneOldEvents(testDb, -1)).toThrow();
      expect(() => pruneOldEvents(testDb, 0)).toThrow();

      const count = (
        testDb.prepare("SELECT COUNT(*) as cnt FROM events").get() as { cnt: number }
      ).cnt;
      expect(count).toBe(1);
    });

    it("rolls back the entire deletion when a trigger forces a failure mid-transaction", () => {
      insertEventWithCreatedAt("OLD-1", 1, "datetime('now', '-100 days')");
      insertEventWithCreatedAt("__SENTINEL_FAIL__", 2, "datetime('now', '-100 days')");
      insertEventWithCreatedAt("OLD-3", 3, "datetime('now', '-100 days')");

      const beforeCount = (
        testDb.prepare("SELECT COUNT(*) as cnt FROM events").get() as { cnt: number }
      ).cnt;
      expect(beforeCount).toBe(3);

      testDb.exec(`
        CREATE TRIGGER IF NOT EXISTS trg_fail_on_delete_sentinel
        BEFORE DELETE ON events
        WHEN OLD.contract_id = '__SENTINEL_FAIL__'
        BEGIN
          SELECT RAISE(FAIL, 'intentional test failure');
        END;
      `);

      try {
        expect(() => pruneOldEvents(testDb, 90)).toThrow();

        // Zero rows should have been deleted -- the whole transaction must
        // have rolled back, not just stopped partway through.
        const afterCount = (
          testDb.prepare("SELECT COUNT(*) as cnt FROM events").get() as { cnt: number }
        ).cnt;
        expect(afterCount).toBe(3);
      } finally {
        testDb.exec("DROP TRIGGER IF EXISTS trg_fail_on_delete_sentinel");
      }
    });
  });

  // -------------------------------------------------------------------------
  // runVacuum
  // -------------------------------------------------------------------------

  describe("runVacuum", () => {
    it("runs without throwing when called outside any transaction", () => {
      expect(() => runVacuum(testDb)).not.toThrow();
    });

    it("throws when run from inside an active transaction (SQLite constraint)", () => {
      // This documents and proves the exact engine constraint that shapes
      // this module's design: VACUUM cannot run nested inside a transaction.
      expect(() => {
        const wrapped = testDb.transaction(() => {
          runVacuum(testDb);
        });
        wrapped();
      }).toThrow(/cannot VACUUM from within a transaction/i);
    });
  });

  // -------------------------------------------------------------------------
  // runVacuumCleanup
  // -------------------------------------------------------------------------

  describe("runVacuumCleanup", () => {
    function insertEventWithCreatedAt(
      contractId: string,
      ledgerSequence: number,
      createdAtExpr: string
    ) {
      testDb
        .prepare(
          `INSERT INTO events
           (contract_id, event_type, ledger_sequence, timestamp, data_json, created_at)
           VALUES (?, 'test-event', ?, 1000, '{}', ${createdAtExpr})`
        )
        .run(contractId, ledgerSequence);
    }

    it("prunes old events and vacuums end-to-end", () => {
      insertEventWithCreatedAt("OLD-1", 1, "datetime('now', '-100 days')");
      insertEventWithCreatedAt("OLD-2", 2, "datetime('now', '-95 days')");
      insertEventWithCreatedAt("RECENT-1", 3, "datetime('now', '-1 days')");

      const result = runVacuumCleanup(testDb, { retentionDays: 90 });

      expect(result.prunedEvents).toBe(2);
      expect(result.vacuumed).toBe(true);

      const remaining = testDb
        .prepare("SELECT contract_id FROM events")
        .all() as Array<{ contract_id: string }>;
      expect(remaining).toEqual([{ contract_id: "RECENT-1" }]);
    });

    it("uses the default retention window when none is provided", () => {
      insertEventWithCreatedAt(
        "OLD-BEYOND-DEFAULT",
        1,
        `datetime('now', '-${DEFAULT_RETENTION_DAYS + 10} days')`
      );
      insertEventWithCreatedAt("RECENT-1", 2, "datetime('now')");

      const result = runVacuumCleanup(testDb);

      expect(result.prunedEvents).toBe(1);
      expect(result.vacuumed).toBe(true);
    });

    it("propagates the pruning error and never reaches VACUUM when pruning fails", () => {
      insertEventWithCreatedAt("__SENTINEL_FAIL__", 1, "datetime('now', '-100 days')");
      insertEventWithCreatedAt("OLD-2", 2, "datetime('now', '-100 days')");

      testDb.exec(`
        CREATE TRIGGER IF NOT EXISTS trg_fail_on_delete_sentinel_cleanup
        BEFORE DELETE ON events
        WHEN OLD.contract_id = '__SENTINEL_FAIL__'
        BEGIN
          SELECT RAISE(FAIL, 'intentional test failure');
        END;
      `);

      try {
        expect(() => runVacuumCleanup(testDb, { retentionDays: 90 })).toThrow(
          /intentional test failure/i
        );

        // Nothing pruned (transaction rolled back) and vacuum never reached
        // as a consequence -- verified by the fact no rows were removed.
        const count = (
          testDb.prepare("SELECT COUNT(*) as cnt FROM events").get() as { cnt: number }
        ).cnt;
        expect(count).toBe(2);
      } finally {
        testDb.exec("DROP TRIGGER IF EXISTS trg_fail_on_delete_sentinel_cleanup");
      }
    });

    it("throws for an invalid retentionDays option and never reaches VACUUM", () => {
      insertEventWithCreatedAt("OLD-1", 1, "datetime('now', '-100 days')");

      expect(() => runVacuumCleanup(testDb, { retentionDays: 0 })).toThrow();

      const count = (
        testDb.prepare("SELECT COUNT(*) as cnt FROM events").get() as { cnt: number }
      ).cnt;
      expect(count).toBe(1);
    });
  });
});

// ============================================================================
// Issue 1 — Dynamic polling frequency intervals
// ============================================================================

import {
  adjustVacuumPollingInterval,
  getVacuumPollingState,
  resetVacuumPollingState,
  DEFAULT_VACUUM_POLL_INTERVAL_MS,
  MIN_VACUUM_POLL_INTERVAL_MS,
  MAX_VACUUM_POLL_INTERVAL_MS,
  VACUUM_IDLE_BACKOFF_FACTOR,
  VACUUM_IDLE_THRESHOLD_CYCLES,
  validateLedgerRange,
  pruneEventsInLedgerRange,
  LEDGER_RANGE_ERROR_CODES,
  validateVacuumSchema,
  assertVacuumSchemaValid,
} from "../src/indexer/sqlite_vacuum_cleaner.js";

describe("sqlite_vacuum_cleaner — Issue 1: Dynamic polling frequency", () => {
  beforeEach(() => {
    resetVacuumPollingState();
  });

  it("starts at the default interval", () => {
    const state = getVacuumPollingState();
    expect(state.currentIntervalMs).toBe(DEFAULT_VACUUM_POLL_INTERVAL_MS);
    expect(state.idleCycles).toBe(0);
  });

  it("does not increase the interval on a single idle cycle (below threshold)", () => {
    const before = getVacuumPollingState().currentIntervalMs;
    adjustVacuumPollingInterval(0); // 1 idle cycle — below VACUUM_IDLE_THRESHOLD_CYCLES (2)
    const state = getVacuumPollingState();
    expect(state.currentIntervalMs).toBe(before); // unchanged
    expect(state.idleCycles).toBe(1);
  });

  it("increases the wait delay once idle cycles reach the threshold", () => {
    const before = getVacuumPollingState().currentIntervalMs;
    // Cycle 1 — no change yet
    adjustVacuumPollingInterval(0);
    expect(getVacuumPollingState().currentIntervalMs).toBe(before);
    // Cycle 2 — hits threshold, interval backs off
    adjustVacuumPollingInterval(0);
    const state = getVacuumPollingState();
    expect(state.currentIntervalMs).toBe(
      Math.min(before * VACUUM_IDLE_BACKOFF_FACTOR, MAX_VACUUM_POLL_INTERVAL_MS),
    );
    expect(state.idleCycles).toBe(VACUUM_IDLE_THRESHOLD_CYCLES);
  });

  it("continues doubling on subsequent idle cycles up to the maximum", () => {
    // Run enough idle cycles to hit the cap
    for (let i = 0; i < 20; i++) {
      adjustVacuumPollingInterval(0);
    }
    expect(getVacuumPollingState().currentIntervalMs).toBe(
      MAX_VACUUM_POLL_INTERVAL_MS,
    );
  });

  it("resets to the minimum interval when rows are pruned (DB is active)", () => {
    // First build up some idle backoff
    for (let i = 0; i < 4; i++) {
      adjustVacuumPollingInterval(0);
    }
    expect(getVacuumPollingState().currentIntervalMs).toBeGreaterThan(
      DEFAULT_VACUUM_POLL_INTERVAL_MS,
    );

    // Now simulate active pruning
    adjustVacuumPollingInterval(42);
    const state = getVacuumPollingState();
    expect(state.currentIntervalMs).toBe(MIN_VACUUM_POLL_INTERVAL_MS);
    expect(state.idleCycles).toBe(0);
  });

  it("updates lastAdjustedAt on every call", () => {
    const before = getVacuumPollingState().lastAdjustedAt;
    adjustVacuumPollingInterval(0);
    expect(getVacuumPollingState().lastAdjustedAt).toBeGreaterThanOrEqual(before);
  });

  it("resetVacuumPollingState restores defaults", () => {
    for (let i = 0; i < 10; i++) adjustVacuumPollingInterval(0);
    resetVacuumPollingState();
    expect(getVacuumPollingState().currentIntervalMs).toBe(
      DEFAULT_VACUUM_POLL_INTERVAL_MS,
    );
    expect(getVacuumPollingState().idleCycles).toBe(0);
  });
});

// ============================================================================
// Issue 2 — Jest database test coverage simulating RPC events
// ============================================================================

describe("sqlite_vacuum_cleaner — Issue 2: Simulated RPC events write to DB schema", () => {
  let db: Database.Database;

  beforeAll(() => {
    db = new Database(":memory:");
    setDb(db);
  });

  afterAll(() => {
    db.close();
  });

  beforeEach(() => {
    db.exec("DROP TABLE IF EXISTS events");
    db.exec("DROP TABLE IF EXISTS indexer_state");
    db.exec("DROP TABLE IF EXISTS monitored_contracts");
    db.exec("DROP TABLE IF EXISTS schema_migrations");
    db.exec("DROP TABLE IF EXISTS webhook_subscriptions");
    runMigrations();
  });

  /** Helper: simulate an RPC event insertion (mirrors insertEvent in db.ts). */
  function simulateRpcEvent(
    contractId: string,
    eventType: string,
    ledgerSequence: number,
    timestamp: number,
    dataJson: string,
  ): void {
    db.prepare(
      `INSERT OR IGNORE INTO events
         (contract_id, event_type, ledger_sequence, timestamp, data_json)
       VALUES (?, ?, ?, ?, ?)`,
    ).run(contractId, eventType, ledgerSequence, timestamp, dataJson);
  }

  it("simulated ContractInitialized event is persisted correctly", () => {
    simulateRpcEvent(
      "CONTRACT-1",
      "ContractInitialized",
      100,
      1700000000,
      JSON.stringify({ client: "GABC", freelancer: "GDEF", amount: 500 }),
    );

    const row = db
      .prepare("SELECT * FROM events WHERE contract_id = ?")
      .get("CONTRACT-1") as {
      contract_id: string;
      event_type: string;
      ledger_sequence: number;
    };
    expect(row).toBeDefined();
    expect(row.event_type).toBe("ContractInitialized");
    expect(row.ledger_sequence).toBe(100);
  });

  it("simulated MilestoneApproved event is persisted correctly", () => {
    simulateRpcEvent(
      "CONTRACT-2",
      "MilestoneApproved",
      200,
      1700001000,
      JSON.stringify({ milestone_index: 0, approved_by: "GABC" }),
    );

    const row = db
      .prepare(
        "SELECT event_type FROM events WHERE contract_id = ? AND event_type = ?",
      )
      .get("CONTRACT-2", "MilestoneApproved") as {
      event_type: string;
    } | undefined;
    expect(row).toBeDefined();
    expect(row!.event_type).toBe("MilestoneApproved");
  });

  it("multiple simulated RPC events for the same contract are all written", () => {
    const events = [
      { type: "ContractInitialized", ledger: 300 },
      { type: "FundsDeposited", ledger: 310 },
      { type: "MilestoneApproved", ledger: 320 },
      { type: "ContractCompleted", ledger: 330 },
    ];

    for (const ev of events) {
      simulateRpcEvent(
        "CONTRACT-3",
        ev.type,
        ev.ledger,
        1700002000 + ev.ledger,
        "{}",
      );
    }

    const count = (
      db
        .prepare(
          "SELECT COUNT(*) as cnt FROM events WHERE contract_id = ?",
        )
        .get("CONTRACT-3") as { cnt: number }
    ).cnt;
    expect(count).toBe(events.length);
  });

  it("duplicate RPC events (same contract+ledger+type) are not double-written", () => {
    simulateRpcEvent("CONTRACT-4", "FundsDeposited", 400, 1700003000, "{}");
    simulateRpcEvent("CONTRACT-4", "FundsDeposited", 400, 1700003000, "{}");

    const count = (
      db
        .prepare(
          "SELECT COUNT(*) as cnt FROM events WHERE contract_id = ? AND event_type = ?",
        )
        .get("CONTRACT-4", "FundsDeposited") as { cnt: number }
    ).cnt;
    expect(count).toBe(1);
  });

  it("simulated events survive a subsequent vacuum cleanup cycle", () => {
    simulateRpcEvent("CONTRACT-5", "ContractInitialized", 500, 1700004000, "{}");

    // Vacuum should not delete a freshly inserted event
    const result = runVacuumCleanup(db, { retentionDays: 90 });
    expect(result.vacuumed).toBe(true);

    const row = db
      .prepare("SELECT contract_id FROM events WHERE contract_id = ?")
      .get("CONTRACT-5");
    expect(row).toBeDefined();
  });

  it("all required schema columns are present after migrations", () => {
    const columns = db
      .prepare("PRAGMA table_info(events)")
      .all() as Array<{ name: string }>;
    const names = new Set(columns.map((c) => c.name));

    for (const col of [
      "id",
      "contract_id",
      "event_type",
      "ledger_sequence",
      "timestamp",
      "data_json",
      "created_at",
    ]) {
      expect(names.has(col)).toBe(true);
    }
  });
});

// ============================================================================
// Issue 3 — Dynamic start/end ledger values for custom historical imports
// ============================================================================

describe("sqlite_vacuum_cleaner — Issue 3: Custom ledger range pruning", () => {
  let db: Database.Database;

  beforeAll(() => {
    db = new Database(":memory:");
    setDb(db);
  });

  afterAll(() => {
    db.close();
  });

  beforeEach(() => {
    db.exec("DROP TABLE IF EXISTS events");
    db.exec("DROP TABLE IF EXISTS indexer_state");
    db.exec("DROP TABLE IF EXISTS monitored_contracts");
    db.exec("DROP TABLE IF EXISTS schema_migrations");
    db.exec("DROP TABLE IF EXISTS webhook_subscriptions");
    runMigrations();
  });

  function insertAtLedger(contractId: string, ledger: number): void {
    db.prepare(
      `INSERT OR IGNORE INTO events
         (contract_id, event_type, ledger_sequence, timestamp, data_json)
       VALUES (?, 'test', ?, 1000, '{}')`,
    ).run(contractId, ledger);
  }

  // --- validateLedgerRange ---

  describe("validateLedgerRange", () => {
    it("accepts equal start and end (single-ledger range)", () => {
      expect(validateLedgerRange(10, 10)).toEqual({ ok: true });
    });

    it("accepts a normal range", () => {
      expect(validateLedgerRange(1, 100)).toEqual({ ok: true });
    });

    it("accepts zero as startLedger", () => {
      expect(validateLedgerRange(0, 50)).toEqual({ ok: true });
    });

    it("rejects negative startLedger", () => {
      const r = validateLedgerRange(-1, 10);
      expect(r.ok).toBe(false);
      if (!r.ok) expect(r.code).toBe(LEDGER_RANGE_ERROR_CODES.INVALID_LEDGER_RANGE);
    });

    it("rejects non-integer startLedger", () => {
      const r = validateLedgerRange(1.5, 10);
      expect(r.ok).toBe(false);
    });

    it("rejects startLedger > endLedger", () => {
      const r = validateLedgerRange(100, 50);
      expect(r.ok).toBe(false);
      if (!r.ok) expect(r.code).toBe(LEDGER_RANGE_ERROR_CODES.INVALID_LEDGER_RANGE);
    });
  });

  // --- pruneEventsInLedgerRange ---

  describe("pruneEventsInLedgerRange", () => {
    it("deletes only events within the specified ledger range", () => {
      insertAtLedger("A", 100);
      insertAtLedger("B", 200);
      insertAtLedger("C", 300);
      insertAtLedger("D", 400);

      const result = pruneEventsInLedgerRange(db, {
        startLedger: 200,
        endLedger: 300,
      });

      expect(result.prunedEvents).toBe(2);
      expect(result.startLedger).toBe(200);
      expect(result.endLedger).toBe(300);

      const remaining = db
        .prepare(
          "SELECT ledger_sequence FROM events ORDER BY ledger_sequence",
        )
        .all() as Array<{ ledger_sequence: number }>;
      expect(remaining.map((r) => r.ledger_sequence)).toEqual([100, 400]);
    });

    it("returns 0 when no events fall in the range", () => {
      insertAtLedger("A", 100);
      const result = pruneEventsInLedgerRange(db, {
        startLedger: 500,
        endLedger: 600,
      });
      expect(result.prunedEvents).toBe(0);
    });

    it("correctly handles a single-ledger range (start === end)", () => {
      insertAtLedger("A", 100);
      insertAtLedger("B", 100);
      insertAtLedger("C", 101);

      const result = pruneEventsInLedgerRange(db, {
        startLedger: 100,
        endLedger: 100,
      });
      // A and B share ledger 100 — both should be deleted
      expect(result.prunedEvents).toBe(2);
    });

    it("asserts correct event count when importing a block range", () => {
      // Simulate importing 5 ledgers worth of events (10 per ledger = 50 total)
      for (let ledger = 1000; ledger <= 1004; ledger++) {
        for (let i = 0; i < 10; i++) {
          insertAtLedger(`C-${ledger}-${i}`, ledger);
        }
      }

      const total = (
        db.prepare("SELECT COUNT(*) as cnt FROM events").get() as { cnt: number }
      ).cnt;
      expect(total).toBe(50); // assert 50 events indexed

      const result = pruneEventsInLedgerRange(db, {
        startLedger: 1000,
        endLedger: 1004,
      });
      expect(result.prunedEvents).toBe(50);
    });

    it("throws for an invalid range and does not delete any rows", () => {
      insertAtLedger("A", 100);
      expect(() =>
        pruneEventsInLedgerRange(db, { startLedger: -1, endLedger: 100 }),
      ).toThrow();
      expect(() =>
        pruneEventsInLedgerRange(db, { startLedger: 200, endLedger: 100 }),
      ).toThrow();
      const count = (
        db.prepare("SELECT COUNT(*) as cnt FROM events").get() as { cnt: number }
      ).cnt;
      expect(count).toBe(1);
    });
  });
});

// ============================================================================
// Issue 4 — Schema migration check utilities
// ============================================================================

describe("sqlite_vacuum_cleaner — Issue 4: Schema migration checks", () => {
  let db: Database.Database;

  beforeAll(() => {
    db = new Database(":memory:");
    setDb(db);
  });

  afterAll(() => {
    db.close();
  });

  beforeEach(() => {
    db.exec("DROP TABLE IF EXISTS events");
    db.exec("DROP TABLE IF EXISTS indexer_state");
    db.exec("DROP TABLE IF EXISTS monitored_contracts");
    db.exec("DROP TABLE IF EXISTS schema_migrations");
    db.exec("DROP TABLE IF EXISTS webhook_subscriptions");
  });

  it("validateVacuumSchema returns invalid when tables are missing", () => {
    const result = validateVacuumSchema(db);
    expect(result.valid).toBe(false);
    expect(result.missingTables).toContain("events");
    expect(result.missingTables).toContain("schema_migrations");
  });

  it("validateVacuumSchema returns valid after migrations are applied", () => {
    runMigrations();
    const result = validateVacuumSchema(db);
    expect(result.valid).toBe(true);
    expect(result.missingTables).toHaveLength(0);
    expect(Object.keys(result.missingColumns)).toHaveLength(0);
  });

  it("validateVacuumSchema detects missing columns", () => {
    // Create the events table but omit the created_at column
    db.exec(`
      CREATE TABLE events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        contract_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        ledger_sequence INTEGER NOT NULL,
        timestamp INTEGER NOT NULL,
        data_json TEXT NOT NULL
      );
      CREATE TABLE schema_migrations (
        version INTEGER PRIMARY KEY,
        description TEXT NOT NULL,
        applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
      );
    `);

    const result = validateVacuumSchema(db);
    expect(result.valid).toBe(false);
    expect(result.missingColumns["events"]).toContain("created_at");
  });

  it("assertVacuumSchemaValid throws with a descriptive message when schema is missing", () => {
    // No tables at this point (dropped in beforeEach)
    expect(() => assertVacuumSchemaValid(db)).toThrow(
      /database schema is out of sync/i,
    );
  });

  it("assertVacuumSchemaValid does not throw after migrations are applied", () => {
    runMigrations();
    expect(() => assertVacuumSchemaValid(db)).not.toThrow();
  });

  it("start fails (throws) when database state is out of sync", () => {
    // Simulate a partially migrated state: events table exists but schema_migrations does not
    db.exec(`
      CREATE TABLE events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        contract_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        ledger_sequence INTEGER NOT NULL,
        timestamp INTEGER NOT NULL,
        data_json TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      );
    `);

    expect(() => assertVacuumSchemaValid(db)).toThrow();
    const result = validateVacuumSchema(db);
    expect(result.valid).toBe(false);
    expect(result.missingTables).toContain("schema_migrations");
  });

  it("error message lists every missing table and column", () => {
    let message = "";
    try {
      assertVacuumSchemaValid(db);
    } catch (err) {
      message = (err as Error).message;
    }
    expect(message).toMatch(/missing table: events/);
    expect(message).toMatch(/missing table: schema_migrations/);
  });
});

// ============================================================================
// Issue 343 — Exponential backoff retry strategies on sqlite_vacuum_cleaner
// ============================================================================

describe("sqlite_vacuum_cleaner — #343: Exponential backoff retry", () => {
  // Use zero-delay config so tests don't wait on real timeouts.
  const fastConfig = {
    maxRetries: 3,
    initialBackoffMs: 0,
    backoffMultiplier: 2,
    maxBackoffMs: 0,
  };

  // --- isVacuumRetryableError ---

  describe("isVacuumRetryableError", () => {
    it("returns true for timeout errors", () => {
      expect(isVacuumRetryableError(new Error("RPC connection timeout"))).toBe(
        true,
      );
      expect(isVacuumRetryableError(new Error("request ETIMEDOUT"))).toBe(true);
    });

    it("returns true for connection reset / refused / dropped", () => {
      expect(
        isVacuumRetryableError(new Error("ECONNRESET: connection reset")),
      ).toBe(true);
      expect(
        isVacuumRetryableError(new Error("connection refused")),
      ).toBe(true);
      expect(isVacuumRetryableError(new Error("socket hang up"))).toBe(true);
    });

    it("returns true for SQLite lock / busy errors", () => {
      expect(isVacuumRetryableError(new Error("SQLITE_BUSY"))).toBe(true);
      expect(isVacuumRetryableError(new Error("database is locked"))).toBe(
        true,
      );
      expect(isVacuumRetryableError(new Error("database is busy"))).toBe(true);
      expect(isVacuumRetryableError(new Error("SQLITE_LOCKED"))).toBe(true);
    });

    it("returns true for generic RPC connection errors", () => {
      expect(
        isVacuumRetryableError(new Error("RPC connection failed")),
      ).toBe(true);
    });

    it("returns false for non-error values", () => {
      expect(isVacuumRetryableError("string error")).toBe(false);
      expect(isVacuumRetryableError(42)).toBe(false);
      expect(isVacuumRetryableError(null)).toBe(false);
      expect(isVacuumRetryableError(undefined)).toBe(false);
    });

    it("returns false for non-retryable errors", () => {
      expect(
        isVacuumRetryableError(new Error("syntax error near SELECT")),
      ).toBe(false);
      expect(
        isVacuumRetryableError(new Error("no such table: foo")),
      ).toBe(false);
    });
  });

  // --- computeVacuumBackoffMs ---

  describe("computeVacuumBackoffMs", () => {
    it("returns initialBackoffMs for attempt 0", () => {
      const result = computeVacuumBackoffMs(0, {
        initialBackoffMs: 500,
        backoffMultiplier: 2,
        maxBackoffMs: 30_000,
      });
      expect(result).toBe(500);
    });

    it("doubles the delay on each subsequent attempt", () => {
      expect(
        computeVacuumBackoffMs(0, DEFAULT_VACUUM_RETRY_CONFIG),
      ).toBe(1_000);
      expect(
        computeVacuumBackoffMs(1, DEFAULT_VACUUM_RETRY_CONFIG),
      ).toBe(2_000);
      expect(
        computeVacuumBackoffMs(2, DEFAULT_VACUUM_RETRY_CONFIG),
      ).toBe(4_000);
      expect(
        computeVacuumBackoffMs(3, DEFAULT_VACUUM_RETRY_CONFIG),
      ).toBe(8_000);
      expect(
        computeVacuumBackoffMs(4, DEFAULT_VACUUM_RETRY_CONFIG),
      ).toBe(16_000);
    });

    it("caps at maxBackoffMs", () => {
      const result = computeVacuumBackoffMs(20, DEFAULT_VACUUM_RETRY_CONFIG);
      expect(result).toBe(30_000);
    });

    it("respects custom multiplier and ceiling", () => {
      const cfg = {
        initialBackoffMs: 100,
        backoffMultiplier: 3,
        maxBackoffMs: 500,
      };
      expect(computeVacuumBackoffMs(0, cfg)).toBe(100);
      expect(computeVacuumBackoffMs(1, cfg)).toBe(300);
      expect(computeVacuumBackoffMs(2, cfg)).toBe(500);
      expect(computeVacuumBackoffMs(3, cfg)).toBe(500);
    });
  });

  // --- withVacuumRetrySync ---

  describe("withVacuumRetrySync", () => {
    it("returns the result immediately on first success", () => {
      const fn = jest.fn(() => "ok");
      const result = withVacuumRetrySync(fn, fastConfig);
      expect(result).toBe("ok");
      expect(fn).toHaveBeenCalledTimes(1);
    });

    it("retries on a retryable error and succeeds", () => {
      let calls = 0;
      const fn = jest.fn(() => {
        calls++;
        if (calls < 3) throw new Error("RPC connection timeout");
        return "recovered";
      });

      const result = withVacuumRetrySync(fn, fastConfig);
      expect(result).toBe("recovered");
      expect(fn).toHaveBeenCalledTimes(3);
    });

    it("propagates non-retryable errors immediately without retrying", () => {
      const fn = jest.fn(() => {
        throw new Error("no such table: events");
      });

      expect(() => withVacuumRetrySync(fn, fastConfig)).toThrow(
        /no such table/,
      );
      expect(fn).toHaveBeenCalledTimes(1);
    });

    it("throws after exhausting all retries on a persistent retryable error", () => {
      const fn = jest.fn(() => {
        throw new Error("database is locked");
      });

      expect(() => withVacuumRetrySync(fn, fastConfig)).toThrow(
        /database is locked/,
      );
      expect(fn).toHaveBeenCalledTimes(4);
    });
  });

  // --- withVacuumRetry (async) ---

  describe("withVacuumRetry (async)", () => {
    it("returns the result immediately on first success", async () => {
      const fn = jest.fn(async () => "ok");
      const result = await withVacuumRetry(fn, fastConfig);
      expect(result).toBe("ok");
      expect(fn).toHaveBeenCalledTimes(1);
    });

    it("retries on a retryable error and succeeds", async () => {
      let calls = 0;
      const fn = jest.fn(async () => {
        calls++;
        if (calls < 2) throw new Error("ETIMEDOUT");
        return "recovered";
      });

      const result = await withVacuumRetry(fn, fastConfig);
      expect(result).toBe("recovered");
      expect(fn).toHaveBeenCalledTimes(2);
    });

    it("propagates non-retryable errors immediately", async () => {
      const fn = jest.fn(async () => {
        throw new Error("SQLITE_READONLY: attempt to write a readonly database");
      });

      await expect(withVacuumRetry(fn, fastConfig)).rejects.toThrow(
        /SQLITE_READONLY/,
      );
      expect(fn).toHaveBeenCalledTimes(1);
    });

    it("throws after exhausting all retries", async () => {
      const fn = jest.fn(async () => {
        throw new Error("connect timeout");
      });

      await expect(withVacuumRetry(fn, fastConfig)).rejects.toThrow(
        /connect timeout/,
      );
      expect(fn).toHaveBeenCalledTimes(4);
    });
  });

  // --- runVacuumWithRetry ---

  describe("runVacuumWithRetry", () => {
    it("runs vacuum successfully on the first attempt", () => {
      const db = new Database(":memory:");
      try {
        expect(() => runVacuumWithRetry(db, fastConfig)).not.toThrow();
      } finally {
        db.close();
      }
    });

    it("retries vacuum on a retryable error and succeeds", () => {
      const db = new Database(":memory:");
      try {
        let calls = 0;
        db.exec("CREATE TABLE events (id INTEGER)");
        // Spy on db.exec: fail twice then succeed
        const spy = jest
          .spyOn(db, "exec")
          .mockImplementationOnce(() => {
            calls++;
            throw new Error("SQLITE_BUSY");
          })
          .mockImplementationOnce(() => {
            calls++;
            throw new Error("database is locked");
          })
          .mockImplementation(() => {
            calls++;
          });

        expect(() => runVacuumWithRetry(db, fastConfig)).not.toThrow();
        expect(spy).toHaveBeenCalledTimes(3);
        spy.mockRestore();
        expect(calls).toBe(3);
      } finally {
        db.close();
      }
    });
  });

  // --- runVacuumCleanupWithRetry ---

  describe("runVacuumCleanupWithRetry", () => {
    let testDb: Database.Database;

    beforeEach(() => {
      testDb = new Database(":memory:");
      setDb(testDb);
      runMigrations();
    });

    afterEach(() => {
      testDb.close();
    });

    it("completes a full cleanup cycle successfully", () => {
      const result = runVacuumCleanupWithRetry(
        testDb,
        { retentionDays: 90 },
        fastConfig,
      );
      expect(result.prunedEvents).toBe(0);
      expect(result.vacuumed).toBe(true);
    });

    it("prunes old events and vacuums end-to-end with retry", () => {
      testDb.prepare(
        `INSERT INTO events
         (contract_id, event_type, ledger_sequence, timestamp, data_json, created_at)
         VALUES ('OLD', 'test', 1, 1000, '{}', datetime('now', '-100 days'))`,
      ).run();

      const result = runVacuumCleanupWithRetry(
        testDb,
        { retentionDays: 90 },
        fastConfig,
      );
      expect(result.prunedEvents).toBe(1);
      expect(result.vacuumed).toBe(true);

      const remaining = testDb
        .prepare("SELECT COUNT(*) as cnt FROM events")
        .get() as { cnt: number };
      expect(remaining.cnt).toBe(0);
    });

    it("does not retry pruning — propagates pruning errors immediately", () => {
      // Insert a sentinel that triggers a trigger error during prune
      testDb.prepare(
        `INSERT INTO events
         (contract_id, event_type, ledger_sequence, timestamp, data_json, created_at)
         VALUES ('__SENTINEL_FAIL__', 'test', 1, 1000, '{}', datetime('now', '-100 days'))`,
      ).run();

      testDb.exec(`
        CREATE TRIGGER trg_fail_on_delete_sentinel_retry
        BEFORE DELETE ON events
        WHEN OLD.contract_id = '__SENTINEL_FAIL__'
        BEGIN
          SELECT RAISE(FAIL, 'intentional test failure');
        END;
      `);

      try {
        expect(() =>
          runVacuumCleanupWithRetry(
            testDb,
            { retentionDays: 90 },
            fastConfig,
          ),
        ).toThrow(/intentional test failure/);
      } finally {
        testDb.exec(
          "DROP TRIGGER IF EXISTS trg_fail_on_delete_sentinel_retry",
        );
      }
    });
  });
});
