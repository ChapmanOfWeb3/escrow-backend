import Database from "better-sqlite3";
import request from "supertest";
import express from "express";
import type { Request, Response } from "express";
import {
  initSchema,
  insertEvent,
  setDb,
  getJobsByWallet,
} from "../src/indexer/db.js";

// ---------------------------------------------------------------------------
// Valid 56-char Stellar G-addresses (pass StrKey.isValidEd25519PublicKey).
// Used in HTTP integration tests where the Zod param schema now validates them.
// GAODBHVR63Z56MVQRBEJSYM2H5423LJ4WAPUUBOFG4JYY72S6ROKVZRX is the well-known
// valid address already used across the entire test suite.
// ---------------------------------------------------------------------------

/** The single well-known valid Stellar account address used project-wide. */
const VALID_G_ADDR = "GAODBHVR63Z56MVQRBEJSYM2H5423LJ4WAPUUBOFG4JYY72S6ROKVZRX";


// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/** Seed a single event into the in-memory DB */
function seedEvent(
  db: Database.Database,
  opts: {
    contractId: string;
    eventType: string;
    ledger: number;
    timestamp: number;
    dataJson: string;
  }
) {
  db.prepare(
    `INSERT OR IGNORE INTO events
       (contract_id, event_type, ledger_sequence, timestamp, data_json)
     VALUES (?, ?, ?, ?, ?)`
  ).run(
    opts.contractId,
    opts.eventType,
    opts.ledger,
    opts.timestamp,
    opts.dataJson
  );
}

// ---------------------------------------------------------------------------
// Shared DB setup
// ---------------------------------------------------------------------------

let testDb: Database.Database;

beforeAll(() => {
  testDb = new Database(":memory:");
  setDb(testDb);
  initSchema();
});

afterAll(() => {
  testDb.close();
});

beforeEach(() => {
  testDb.exec("DELETE FROM events");
});

// ---------------------------------------------------------------------------
// Unit tests: getJobsByWallet()
// ---------------------------------------------------------------------------

describe("getJobsByWallet() – unit", () => {
  const CLIENT = "GCLIENT111";
  const FREELANCER = "GFREELANCER222";
  const ARBITER = "GARBITER333";
  const CONTRACT_A = "CONTRACT-A";
  const CONTRACT_B = "CONTRACT-B";
  const CONTRACT_C = "CONTRACT-C";

  it("returns empty result when no events exist for address", () => {
    const result = getJobsByWallet("GNOBODY");
    expect(result.total).toBe(0);
    expect(result.jobs).toHaveLength(0);
    expect(result.page).toBe(1);
    expect(result.limit).toBe(10);
  });

  it("returns a job where address is the CLIENT", () => {
    seedEvent(testDb, {
      contractId: CONTRACT_A,
      eventType: "initialized",
      ledger: 100,
      timestamp: 1000,
      dataJson: JSON.stringify({ client: CLIENT, freelancer: FREELANCER, arbiter: ARBITER }),
    });

    const result = getJobsByWallet(CLIENT);
    expect(result.total).toBe(1);
    expect(result.jobs[0].contract_id).toBe(CONTRACT_A);
    expect(result.jobs[0].role).toBe("client");
  });

  it("returns a job where address is the FREELANCER", () => {
    seedEvent(testDb, {
      contractId: CONTRACT_B,
      eventType: "funded",
      ledger: 200,
      timestamp: 2000,
      dataJson: JSON.stringify({ client: CLIENT, freelancer: FREELANCER }),
    });

    const result = getJobsByWallet(FREELANCER);
    expect(result.total).toBe(1);
    expect(result.jobs[0].contract_id).toBe(CONTRACT_B);
    expect(result.jobs[0].role).toBe("freelancer");
  });

  it("returns a job where address is the ARBITER", () => {
    seedEvent(testDb, {
      contractId: CONTRACT_C,
      eventType: "dispute_raised",
      ledger: 300,
      timestamp: 3000,
      dataJson: JSON.stringify({ arbiter: ARBITER }),
    });

    const result = getJobsByWallet(ARBITER);
    expect(result.total).toBe(1);
    expect(result.jobs[0].contract_id).toBe(CONTRACT_C);
    expect(result.jobs[0].role).toBe("arbiter");
  });

  it("groups multiple events for the same contract_id into one job", () => {
    // Two events, same contract, same freelancer
    seedEvent(testDb, {
      contractId: CONTRACT_A,
      eventType: "initialized",
      ledger: 100,
      timestamp: 1000,
      dataJson: JSON.stringify({ freelancer: FREELANCER }),
    });
    seedEvent(testDb, {
      contractId: CONTRACT_A,
      eventType: "funded",
      ledger: 101,
      timestamp: 1001,
      dataJson: JSON.stringify({ freelancer: FREELANCER }),
    });

    const result = getJobsByWallet(FREELANCER);
    expect(result.total).toBe(1);
    // Should capture the most-recent event type (highest ledger comes first)
    expect(result.jobs[0].latest_event_type).toBe("funded");
  });

  it("returns distinct jobs across multiple contracts", () => {
    const addr = "GMULTICONTRACT";
    seedEvent(testDb, {
      contractId: "C1",
      eventType: "initialized",
      ledger: 10,
      timestamp: 100,
      dataJson: JSON.stringify({ client: addr }),
    });
    seedEvent(testDb, {
      contractId: "C2",
      eventType: "funded",
      ledger: 20,
      timestamp: 200,
      dataJson: JSON.stringify({ client: addr }),
    });
    seedEvent(testDb, {
      contractId: "C3",
      eventType: "approved",
      ledger: 30,
      timestamp: 300,
      dataJson: JSON.stringify({ client: addr }),
    });

    const result = getJobsByWallet(addr);
    expect(result.total).toBe(3);
  });

  it("does not match address that only appears in non-role fields", () => {
    const addr = "GNOTAROLE";
    seedEvent(testDb, {
      contractId: "C-FAKE",
      eventType: "initialized",
      ledger: 50,
      timestamp: 500,
      dataJson: JSON.stringify({ token: addr, some_other_field: addr }),
    });

    const result = getJobsByWallet(addr);
    expect(result.total).toBe(0);
  });

  // -------------------------------------------------------------------------
  // Pagination
  // -------------------------------------------------------------------------

  it("pagination: page=1 limit=2 returns first 2 of 5 jobs", () => {
    const addr = "GPAGER";
    for (let i = 1; i <= 5; i++) {
      seedEvent(testDb, {
        contractId: `C${i}`,
        eventType: "initialized",
        ledger: i * 10,
        timestamp: i * 100,
        dataJson: JSON.stringify({ client: addr }),
      });
    }

    const p1 = getJobsByWallet(addr, 1, 2);
    expect(p1.total).toBe(5);
    expect(p1.jobs).toHaveLength(2);
    expect(p1.page).toBe(1);
    expect(p1.limit).toBe(2);
  });

  it("pagination: page=2 limit=2 returns jobs 3-4 of 5", () => {
    const addr = "GPAGER2";
    for (let i = 1; i <= 5; i++) {
      seedEvent(testDb, {
        contractId: `D${i}`,
        eventType: "initialized",
        ledger: i * 10,
        timestamp: i * 100,
        dataJson: JSON.stringify({ client: addr }),
      });
    }

    const p2 = getJobsByWallet(addr, 2, 2);
    expect(p2.total).toBe(5);
    expect(p2.jobs).toHaveLength(2);
    expect(p2.page).toBe(2);
  });

  it("pagination: last page returns remaining jobs (not a full page)", () => {
    const addr = "GPAGER3";
    for (let i = 1; i <= 5; i++) {
      seedEvent(testDb, {
        contractId: `E${i}`,
        eventType: "initialized",
        ledger: i * 10,
        timestamp: i * 100,
        dataJson: JSON.stringify({ client: addr }),
      });
    }

    const p3 = getJobsByWallet(addr, 3, 2);
    expect(p3.total).toBe(5);
    expect(p3.jobs).toHaveLength(1); // page 3 of 2-per-page = only 1 left
  });

  it("pagination: page beyond total returns empty jobs array", () => {
    const addr = "GPAGER4";
    seedEvent(testDb, {
      contractId: "F1",
      eventType: "initialized",
      ledger: 10,
      timestamp: 100,
      dataJson: JSON.stringify({ client: addr }),
    });

    const p = getJobsByWallet(addr, 99, 10);
    expect(p.total).toBe(1);
    expect(p.jobs).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// HTTP integration tests: GET /api/jobs/by-wallet/:address
// ---------------------------------------------------------------------------

describe("GET /api/jobs/by-wallet/:address – HTTP", () => {
  let app: express.Express;

  beforeAll(async () => {
    // Dynamically import the router AFTER setDb() so it uses the in-memory DB
    const { default: router } = await import("../src/routes/jobs.js");
    app = express();
    app.use(express.json());
    app.use("/api/jobs", router);
  });

  it("returns success:true with jobs array and pagination fields", async () => {
    seedEvent(testDb, {
      contractId: "HTTP-C1",
      eventType: "initialized",
      ledger: 1,
      timestamp: 100,
      dataJson: JSON.stringify({ client: VALID_G_ADDR }),
    });

    const res = await request(app)
      .get(`/api/jobs/by-wallet/${VALID_G_ADDR}`)
      .expect(200);

    expect(res.body.success).toBe(true);
    expect(Array.isArray(res.body.jobs)).toBe(true);
    expect(res.body.total).toBeDefined();
    expect(res.body.page).toBeDefined();
    expect(res.body.limit).toBeDefined();
  });

  it("returns empty jobs array for unknown address", async () => {
    const res = await request(app)
      .get(`/api/jobs/by-wallet/${VALID_G_ADDR}`)
      .expect(200);

    expect(res.body.success).toBe(true);
    expect(res.body.jobs).toHaveLength(0);
    expect(res.body.total).toBe(0);
  });

  it("respects ?page=1&limit=2 query params", async () => {
    for (let i = 1; i <= 4; i++) {
      seedEvent(testDb, {
        contractId: `HP${i}`,
        eventType: "initialized",
        ledger: i,
        timestamp: i * 100,
        dataJson: JSON.stringify({ client: VALID_G_ADDR }),
      });
    }

    const res = await request(app)
      .get(`/api/jobs/by-wallet/${VALID_G_ADDR}?page=1&limit=2`)
      .expect(200);

    expect(res.body.success).toBe(true);
    expect(res.body.jobs).toHaveLength(2);
    expect(res.body.total).toBe(4);
    expect(res.body.page).toBe(1);
    expect(res.body.limit).toBe(2);
  });

  it("each job entry has the expected shape", async () => {
    seedEvent(testDb, {
      contractId: "SHAPE-C",
      eventType: "funded",
      ledger: 50,
      timestamp: 5000,
      dataJson: JSON.stringify({ freelancer: VALID_G_ADDR }),
    });

    const res = await request(app)
      .get(`/api/jobs/by-wallet/${VALID_G_ADDR}`)
      .expect(200);

    const job = res.body.jobs[0];
    expect(job).toMatchObject({
      contract_id: expect.any(String),
      role: expect.stringMatching(/^(client|freelancer|arbiter)$/),
      milestone_count: expect.any(Number),
      latest_event_type: expect.any(String),
      latest_ledger: expect.any(Number),
      latest_timestamp: expect.any(Number),
    });
  });

  // -------------------------------------------------------------------------
  // Validation error tests (new Zod-based param / query validation)
  // -------------------------------------------------------------------------

  it("returns 400 with success:false for an invalid (non-Stellar) address", async () => {
    const res = await request(app)
      .get("/api/jobs/by-wallet/not-a-stellar-address")
      .expect(400);

    expect(res.body.success).toBe(false);
    expect(typeof res.body.error).toBe("string");
    expect(res.body.error).toMatch(/address/i);
    // Must never expose internal stack traces
    expect(res.body).not.toHaveProperty("stack");
  });

  it("returns 400 with success:false when page=0 (below minimum)", async () => {
    const res = await request(app)
      .get(`/api/jobs/by-wallet/${VALID_G_ADDR}?page=0`)
      .expect(400);

    expect(res.body.success).toBe(false);
    expect(res.body.error).toMatch(/page must be a positive integer/i);
    expect(res.body).not.toHaveProperty("stack");
  });

  it("returns 400 with success:false when limit=0", async () => {
    const res = await request(app)
      .get(`/api/jobs/by-wallet/${VALID_G_ADDR}?limit=0`)
      .expect(400);

    expect(res.body.success).toBe(false);
    expect(res.body.error).toMatch(/limit must be between 1 and 100/i);
    expect(res.body).not.toHaveProperty("stack");
  });

  it("returns 400 with success:false when limit=101 (above maximum)", async () => {
    const res = await request(app)
      .get(`/api/jobs/by-wallet/${VALID_G_ADDR}?limit=101`)
      .expect(400);

    expect(res.body.success).toBe(false);
    expect(res.body.error).toMatch(/limit must be between 1 and 100/i);
    expect(res.body).not.toHaveProperty("stack");
  });

  it("returns 400 with success:false when page is not a number", async () => {
    const res = await request(app)
      .get(`/api/jobs/by-wallet/${VALID_G_ADDR}?page=abc`)
      .expect(400);

    expect(res.body.success).toBe(false);
    expect(res.body).not.toHaveProperty("stack");
  });

  // -------------------------------------------------------------------------
  // 500 error path – server errors produce a clean response without stack traces
  // -------------------------------------------------------------------------

  it("returns 500 with success:false when getJobsByWallet throws, without leaking stack traces", async () => {
    // Build a fresh app that mounts a router with a mock DB that always throws
    const { default: router } = await import("../src/routes/jobs.js");

    // Temporarily break the DB to force getJobsByWallet to throw
    testDb.exec("DROP TABLE events");

    try {
      const errorApp = express();
      errorApp.use(express.json());
      errorApp.use("/api/jobs", router);

      const res = await request(errorApp)
        .get(`/api/jobs/by-wallet/${VALID_G_ADDR}`)
        .expect(500);

      expect(res.body.success).toBe(false);
      expect(res.body.error).toBe("Internal server error");
      // Stack traces and internal messages must never reach the client
      expect(res.body).not.toHaveProperty("stack");
      expect(JSON.stringify(res.body)).not.toMatch(/events/); // no SQL table name
    } finally {
      // Restore the events table for subsequent tests
      testDb.exec(`
        CREATE TABLE IF NOT EXISTS events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          contract_id TEXT NOT NULL,
          event_type TEXT NOT NULL,
          ledger_sequence INTEGER NOT NULL,
          timestamp INTEGER NOT NULL,
          data_json TEXT NOT NULL,
          created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
          UNIQUE(contract_id, ledger_sequence, event_type)
        )
      `);
    }
  });
});
