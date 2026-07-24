import Database from "better-sqlite3";
import request from "supertest";
import express from "express";
import {
  initSchema,
  insertEvent,
  setDb,
  getJobsByWallet,
} from "../src/indexer/db.js";

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
    expect(result.jobs[0].latest_event_type).toBe("funded");
  });

  it("returns distinct jobs across multiple contracts", () => {
    const addr = "GMULTICONTRACT";
    seedEvent(testDb, { contractId: "C1", eventType: "initialized", ledger: 10, timestamp: 100, dataJson: JSON.stringify({ client: addr }) });
    seedEvent(testDb, { contractId: "C2", eventType: "funded",      ledger: 20, timestamp: 200, dataJson: JSON.stringify({ client: addr }) });
    seedEvent(testDb, { contractId: "C3", eventType: "approved",    ledger: 30, timestamp: 300, dataJson: JSON.stringify({ client: addr }) });

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

  it("correctly extracts milestone_count from data_json milestones array", () => {
    const addr = "GMILESTONETEST";
    const milestones = [{ amount: "100" }, { amount: "200" }, { amount: "300" }];
    seedEvent(testDb, {
      contractId: "CONTRACT-MS",
      eventType: "initialized",
      ledger: 10,
      timestamp: 100,
      dataJson: JSON.stringify({ client: addr, milestones }),
    });

    const result = getJobsByWallet(addr);
    expect(result.jobs[0].milestone_count).toBe(3);
  });

  it("returns milestone_count=0 when data_json has no milestones field", () => {
    const addr = "GNOMILESTONES";
    seedEvent(testDb, {
      contractId: "CONTRACT-NMS",
      eventType: "initialized",
      ledger: 10,
      timestamp: 100,
      dataJson: JSON.stringify({ client: addr }),
    });

    const result = getJobsByWallet(addr);
    expect(result.jobs[0].milestone_count).toBe(0);
  });

  it("returns milestone_count=0 when milestones field is not an array", () => {
    const addr = "GBADMILESTONES";
    seedEvent(testDb, {
      contractId: "CONTRACT-BMS",
      eventType: "initialized",
      ledger: 10,
      timestamp: 100,
      dataJson: JSON.stringify({ client: addr, milestones: "not-an-array" }),
    });

    const result = getJobsByWallet(addr);
    expect(result.jobs[0].milestone_count).toBe(0);
  });

  it("role priority: client takes precedence when address matches all three roles in same event", () => {
    const addr = "GMULTIROLE";
    seedEvent(testDb, {
      contractId: "CONTRACT-MULTI",
      eventType: "initialized",
      ledger: 10,
      timestamp: 100,
      dataJson: JSON.stringify({ client: addr, freelancer: addr, arbiter: addr }),
    });

    const result = getJobsByWallet(addr);
    expect(result.total).toBe(1);
    // CASE expression checks client first → role = "client"
    expect(result.jobs[0].role).toBe("client");
  });

  it("each job has all required JobSummary fields with correct types", () => {
    const addr = "GSHAPEUNIT";
    seedEvent(testDb, {
      contractId: "CONTRACT-SHAPE",
      eventType: "funded",
      ledger: 42,
      timestamp: 4200,
      dataJson: JSON.stringify({ freelancer: addr }),
    });

    const result = getJobsByWallet(addr);
    const job = result.jobs[0];
    expect(typeof job.contract_id).toBe("string");
    expect(["client", "freelancer", "arbiter", "unknown"]).toContain(job.role);
    expect(typeof job.milestone_count).toBe("number");
    expect(typeof job.latest_event_type).toBe("string");
    expect(typeof job.latest_ledger).toBe("number");
    expect(typeof job.latest_timestamp).toBe("number");
  });

  it("latest_ledger and latest_timestamp reflect the most-recent event", () => {
    const addr = "GLATESTCHECK";
    seedEvent(testDb, { contractId: "CONTRACT-LC", eventType: "initialized", ledger: 10, timestamp: 1000, dataJson: JSON.stringify({ client: addr }) });
    seedEvent(testDb, { contractId: "CONTRACT-LC", eventType: "funded",      ledger: 50, timestamp: 5000, dataJson: JSON.stringify({ client: addr }) });
    seedEvent(testDb, { contractId: "CONTRACT-LC", eventType: "approved",    ledger: 30, timestamp: 3000, dataJson: JSON.stringify({ client: addr }) });

    const result = getJobsByWallet(addr);
    expect(result.jobs[0].latest_ledger).toBe(50);
    expect(result.jobs[0].latest_timestamp).toBe(5000);
    expect(result.jobs[0].latest_event_type).toBe("funded");
  });

  // -------------------------------------------------------------------------
  // Pagination – unit layer
  // -------------------------------------------------------------------------

  it("pagination: page=1 limit=2 returns first 2 of 5 jobs", () => {
    const addr = "GPAGER";
    for (let i = 1; i <= 5; i++) {
      seedEvent(testDb, { contractId: `C${i}`, eventType: "initialized", ledger: i * 10, timestamp: i * 100, dataJson: JSON.stringify({ client: addr }) });
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
      seedEvent(testDb, { contractId: `D${i}`, eventType: "initialized", ledger: i * 10, timestamp: i * 100, dataJson: JSON.stringify({ client: addr }) });
    }

    const p2 = getJobsByWallet(addr, 2, 2);
    expect(p2.total).toBe(5);
    expect(p2.jobs).toHaveLength(2);
    expect(p2.page).toBe(2);
  });

  it("pagination: last page returns remaining jobs (not a full page)", () => {
    const addr = "GPAGER3";
    for (let i = 1; i <= 5; i++) {
      seedEvent(testDb, { contractId: `E${i}`, eventType: "initialized", ledger: i * 10, timestamp: i * 100, dataJson: JSON.stringify({ client: addr }) });
    }

    const p3 = getJobsByWallet(addr, 3, 2);
    expect(p3.total).toBe(5);
    expect(p3.jobs).toHaveLength(1);
  });

  it("pagination: page beyond total returns empty jobs array", () => {
    const addr = "GPAGER4";
    seedEvent(testDb, { contractId: "F1", eventType: "initialized", ledger: 10, timestamp: 100, dataJson: JSON.stringify({ client: addr }) });

    const p = getJobsByWallet(addr, 99, 10);
    expect(p.total).toBe(1);
    expect(p.jobs).toHaveLength(0);
  });

  it("pagination: page=3 limit=10 returns correct page/limit in result", () => {
    const addr = "GPAGER5";
    seedEvent(testDb, { contractId: "G1", eventType: "initialized", ledger: 10, timestamp: 100, dataJson: JSON.stringify({ client: addr }) });

    const result = getJobsByWallet(addr, 3, 10);
    expect(result.page).toBe(3);
    expect(result.limit).toBe(10);
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

  // -------------------------------------------------------------------------
  // Success paths
  // -------------------------------------------------------------------------

  it("200: returns success:true with jobs array and pagination fields", async () => {
    const addr = "GHTTPTEST1";
    seedEvent(testDb, { contractId: "HTTP-C1", eventType: "initialized", ledger: 1, timestamp: 100, dataJson: JSON.stringify({ client: addr }) });

    const res = await request(app)
      .get(`/api/jobs/by-wallet/${addr}`)
      .expect(200);

    expect(res.body.success).toBe(true);
    expect(Array.isArray(res.body.jobs)).toBe(true);
    expect(typeof res.body.total).toBe("number");
    expect(typeof res.body.page).toBe("number");
    expect(typeof res.body.limit).toBe("number");
  });

  it("200: response is NOT wrapped in a 'data' envelope (spreads directly)", async () => {
    const addr = "GHTTPENVELOPE";
    seedEvent(testDb, { contractId: "ENV-C1", eventType: "initialized", ledger: 1, timestamp: 100, dataJson: JSON.stringify({ client: addr }) });

    const res = await request(app).get(`/api/jobs/by-wallet/${addr}`).expect(200);

    // jobs, total, page, limit are top-level – NOT nested under res.body.data
    expect(res.body.data).toBeUndefined();
    expect(res.body.jobs).toBeDefined();
    expect(res.body.total).toBeDefined();
    expect(res.body.page).toBeDefined();
    expect(res.body.limit).toBeDefined();
  });

  it("200: returns empty jobs array and total=0 for unknown address", async () => {
    const res = await request(app)
      .get("/api/jobs/by-wallet/GNOBODYKNOWSME")
      .expect(200);

    expect(res.body.success).toBe(true);
    expect(res.body.jobs).toHaveLength(0);
    expect(res.body.total).toBe(0);
  });

  it("200: returns a job with role=client when address is the client", async () => {
    const addr = "GCLIENTHTTP";
    seedEvent(testDb, { contractId: "HTTP-CLIENT", eventType: "initialized", ledger: 1, timestamp: 100, dataJson: JSON.stringify({ client: addr, freelancer: "GOTHER", arbiter: "GOTHER2" }) });

    const res = await request(app).get(`/api/jobs/by-wallet/${addr}`).expect(200);

    expect(res.body.jobs[0].role).toBe("client");
    expect(res.body.jobs[0].contract_id).toBe("HTTP-CLIENT");
  });

  it("200: returns a job with role=freelancer when address is the freelancer", async () => {
    const addr = "GFREELANCERHTTP";
    seedEvent(testDb, { contractId: "HTTP-FREELANCER", eventType: "funded", ledger: 2, timestamp: 200, dataJson: JSON.stringify({ client: "GOTHER", freelancer: addr }) });

    const res = await request(app).get(`/api/jobs/by-wallet/${addr}`).expect(200);

    expect(res.body.jobs[0].role).toBe("freelancer");
  });

  it("200: returns a job with role=arbiter when address is the arbiter", async () => {
    const addr = "GARBITERHTTP";
    seedEvent(testDb, { contractId: "HTTP-ARBITER", eventType: "dispute_raised", ledger: 3, timestamp: 300, dataJson: JSON.stringify({ arbiter: addr }) });

    const res = await request(app).get(`/api/jobs/by-wallet/${addr}`).expect(200);

    expect(res.body.jobs[0].role).toBe("arbiter");
  });

  it("200: each job entry has all required fields with correct types", async () => {
    const addr = "GSHAPETEST";
    seedEvent(testDb, { contractId: "SHAPE-C", eventType: "funded", ledger: 50, timestamp: 5000, dataJson: JSON.stringify({ freelancer: addr }) });

    const res = await request(app).get(`/api/jobs/by-wallet/${addr}`).expect(200);

    const job = res.body.jobs[0];
    expect(job).toMatchObject({
      contract_id:        expect.any(String),
      role:               expect.stringMatching(/^(client|freelancer|arbiter|unknown)$/),
      milestone_count:    expect.any(Number),
      latest_event_type:  expect.any(String),
      latest_ledger:      expect.any(Number),
      latest_timestamp:   expect.any(Number),
    });
  });

  it("200: milestone_count reflects milestones array length in data_json", async () => {
    const addr = "GMSCOUNT";
    const milestones = [{ amount: "100" }, { amount: "200" }];
    seedEvent(testDb, { contractId: "MS-COUNT-C", eventType: "initialized", ledger: 1, timestamp: 100, dataJson: JSON.stringify({ client: addr, milestones }) });

    const res = await request(app).get(`/api/jobs/by-wallet/${addr}`).expect(200);

    expect(res.body.jobs[0].milestone_count).toBe(2);
  });

  it("200: default page=1 and limit=10 when no query params supplied", async () => {
    const addr = "GDEFAULTPAGE";
    seedEvent(testDb, { contractId: "DEF-C1", eventType: "initialized", ledger: 1, timestamp: 100, dataJson: JSON.stringify({ client: addr }) });

    const res = await request(app).get(`/api/jobs/by-wallet/${addr}`).expect(200);

    expect(res.body.page).toBe(1);
    expect(res.body.limit).toBe(10);
  });

  it("200: jobs are not present in result when address only appears in non-role fields", async () => {
    const addr = "GNONROLEHTTP";
    seedEvent(testDb, { contractId: "NON-ROLE-C", eventType: "initialized", ledger: 10, timestamp: 100, dataJson: JSON.stringify({ token: addr }) });

    const res = await request(app).get(`/api/jobs/by-wallet/${addr}`).expect(200);

    expect(res.body.jobs).toHaveLength(0);
    expect(res.body.total).toBe(0);
  });

  // -------------------------------------------------------------------------
  // Pagination – HTTP layer
  // -------------------------------------------------------------------------

  it("200: ?page=1&limit=2 returns first 2 of 4 jobs", async () => {
    const addr = "GHTTPPAGE";
    for (let i = 1; i <= 4; i++) {
      seedEvent(testDb, { contractId: `HP${i}`, eventType: "initialized", ledger: i, timestamp: i * 100, dataJson: JSON.stringify({ client: addr }) });
    }

    const res = await request(app).get(`/api/jobs/by-wallet/${addr}?page=1&limit=2`).expect(200);

    expect(res.body.success).toBe(true);
    expect(res.body.jobs).toHaveLength(2);
    expect(res.body.total).toBe(4);
    expect(res.body.page).toBe(1);
    expect(res.body.limit).toBe(2);
  });

  it("200: ?page=2&limit=2 returns jobs 3-4 of 4", async () => {
    const addr = "GHTTPAGE2";
    for (let i = 1; i <= 4; i++) {
      seedEvent(testDb, { contractId: `PG2-${i}`, eventType: "initialized", ledger: i, timestamp: i * 100, dataJson: JSON.stringify({ client: addr }) });
    }

    const res = await request(app).get(`/api/jobs/by-wallet/${addr}?page=2&limit=2`).expect(200);

    expect(res.body.jobs).toHaveLength(2);
    expect(res.body.total).toBe(4);
    expect(res.body.page).toBe(2);
    expect(res.body.limit).toBe(2);
  });

  it("200: page beyond total returns empty jobs array with correct total", async () => {
    const addr = "GBEYONDPAGE";
    seedEvent(testDb, { contractId: "BEYOND-C1", eventType: "initialized", ledger: 1, timestamp: 100, dataJson: JSON.stringify({ client: addr }) });

    const res = await request(app).get(`/api/jobs/by-wallet/${addr}?page=99&limit=10`).expect(200);

    expect(res.body.success).toBe(true);
    expect(res.body.jobs).toHaveLength(0);
    expect(res.body.total).toBe(1);
    expect(res.body.page).toBe(99);
  });

  it("200: ?limit=100 (maximum allowed) is accepted", async () => {
    const addr = "GMAXLIMIT";
    seedEvent(testDb, { contractId: "MAX-C1", eventType: "initialized", ledger: 1, timestamp: 100, dataJson: JSON.stringify({ client: addr }) });

    const res = await request(app).get(`/api/jobs/by-wallet/${addr}?limit=100`).expect(200);

    expect(res.body.success).toBe(true);
    expect(res.body.limit).toBe(100);
  });

  // -------------------------------------------------------------------------
  // Validation error paths – 400
  // -------------------------------------------------------------------------

  it("400: invalid page (page=0) returns error with correct message", async () => {
    const res = await request(app)
      .get("/api/jobs/by-wallet/GSOMEADDR?page=0")
      .expect(400);

    expect(res.body).toEqual({
      success: false,
      error: "page must be a positive integer",
    });
  });

  it("400: invalid page (page=-1) returns 400", async () => {
    const res = await request(app)
      .get("/api/jobs/by-wallet/GSOMEADDR?page=-1")
      .expect(400);

    expect(res.body.success).toBe(false);
    expect(res.body.error).toBe("page must be a positive integer");
  });

  it("400: non-numeric page (page=abc) returns 400", async () => {
    const res = await request(app)
      .get("/api/jobs/by-wallet/GSOMEADDR?page=abc")
      .expect(400);

    expect(res.body.success).toBe(false);
    expect(res.body.error).toBe("page must be a positive integer");
  });

  it("400: limit=0 returns error with correct message", async () => {
    const res = await request(app)
      .get("/api/jobs/by-wallet/GSOMEADDR?limit=0")
      .expect(400);

    expect(res.body).toEqual({
      success: false,
      error: "limit must be between 1 and 100",
    });
  });

  it("400: limit=-5 returns 400", async () => {
    const res = await request(app)
      .get("/api/jobs/by-wallet/GSOMEADDR?limit=-5")
      .expect(400);

    expect(res.body.success).toBe(false);
    expect(res.body.error).toBe("limit must be between 1 and 100");
  });

  it("400: limit=101 (above maximum) returns 400", async () => {
    const res = await request(app)
      .get("/api/jobs/by-wallet/GSOMEADDR?limit=101")
      .expect(400);

    expect(res.body).toEqual({
      success: false,
      error: "limit must be between 1 and 100",
    });
  });

  it("400: non-numeric limit (limit=xyz) returns 400", async () => {
    const res = await request(app)
      .get("/api/jobs/by-wallet/GSOMEADDR?limit=xyz")
      .expect(400);

    expect(res.body.success).toBe(false);
    expect(res.body.error).toBe("limit must be between 1 and 100");
  });

  it("400: both page and limit invalid – page validation fires first", async () => {
    const res = await request(app)
      .get("/api/jobs/by-wallet/GSOMEADDR?page=0&limit=0")
      .expect(400);

    expect(res.body.success).toBe(false);
    // page is checked before limit in the handler
    expect(res.body.error).toBe("page must be a positive integer");
  });

  it("400: error response body always has success:false and an error string", async () => {
    const res = await request(app)
      .get("/api/jobs/by-wallet/GSOMEADDR?page=0")
      .expect(400);

    expect(res.body).toMatchObject({
      success: false,
      error: expect.any(String),
    });
  });

  // -------------------------------------------------------------------------
  // Response headers
  // -------------------------------------------------------------------------

  it("responds with Content-Type: application/json", async () => {
    const res = await request(app)
      .get("/api/jobs/by-wallet/GHEADERTEST")
      .expect(200);

    expect(res.headers["content-type"]).toMatch(/application\/json/);
  });

  it("responds with Content-Type: application/json on 400 errors too", async () => {
    const res = await request(app)
      .get("/api/jobs/by-wallet/GHEADERTEST?page=0")
      .expect(400);

    expect(res.headers["content-type"]).toMatch(/application\/json/);
  });

  // -------------------------------------------------------------------------
  // Multi-role and data integrity
  // -------------------------------------------------------------------------

  it("200: multiple contracts for same address all appear in jobs list", async () => {
    const addr = "GMANYCONTRACTS";
    for (let i = 1; i <= 3; i++) {
      seedEvent(testDb, {
        contractId: `MULTI-${i}`,
        eventType: "initialized",
        ledger: i,
        timestamp: i * 100,
        dataJson: JSON.stringify({ client: addr }),
      });
    }

    const res = await request(app).get(`/api/jobs/by-wallet/${addr}`).expect(200);

    expect(res.body.total).toBe(3);
    expect(res.body.jobs).toHaveLength(3);
    const contractIds = res.body.jobs.map((j: any) => j.contract_id);
    expect(contractIds).toContain("MULTI-1");
    expect(contractIds).toContain("MULTI-2");
    expect(contractIds).toContain("MULTI-3");
  });

  it("200: total in response matches jobs count for small result set", async () => {
    const addr = "GTOTALCHECK";
    for (let i = 1; i <= 3; i++) {
      seedEvent(testDb, { contractId: `TC-${i}`, eventType: "initialized", ledger: i, timestamp: i * 100, dataJson: JSON.stringify({ arbiter: addr }) });
    }

    const res = await request(app).get(`/api/jobs/by-wallet/${addr}`).expect(200);

    expect(res.body.total).toBe(res.body.jobs.length);
  });

  it("200: total reflects full count even when limit restricts returned jobs", async () => {
    const addr = "GTOTALVSLIMIT";
    for (let i = 1; i <= 5; i++) {
      seedEvent(testDb, { contractId: `TVL-${i}`, eventType: "initialized", ledger: i, timestamp: i * 100, dataJson: JSON.stringify({ client: addr }) });
    }

    const res = await request(app).get(`/api/jobs/by-wallet/${addr}?page=1&limit=2`).expect(200);

    expect(res.body.total).toBe(5);
    expect(res.body.jobs).toHaveLength(2);
  });

  it("200: jobs from different addresses are isolated – address A cannot see address B's jobs", async () => {
    const addrA = "GISOLATIONADDR-A";
    const addrB = "GISOLATIONADDR-B";
    seedEvent(testDb, { contractId: "ISO-CA", eventType: "initialized", ledger: 1, timestamp: 100, dataJson: JSON.stringify({ client: addrA }) });
    seedEvent(testDb, { contractId: "ISO-CB", eventType: "initialized", ledger: 2, timestamp: 200, dataJson: JSON.stringify({ client: addrB }) });

    const resA = await request(app).get(`/api/jobs/by-wallet/${addrA}`).expect(200);
    const resB = await request(app).get(`/api/jobs/by-wallet/${addrB}`).expect(200);

    expect(resA.body.total).toBe(1);
    expect(resA.body.jobs[0].contract_id).toBe("ISO-CA");

    expect(resB.body.total).toBe(1);
    expect(resB.body.jobs[0].contract_id).toBe("ISO-CB");
  });

  it("200: latest_event_type reflects the event with highest ledger_sequence", async () => {
    const addr = "GLATESTHTTP";
    seedEvent(testDb, { contractId: "LATEST-HTTP", eventType: "initialized", ledger: 10, timestamp: 1000, dataJson: JSON.stringify({ client: addr }) });
    seedEvent(testDb, { contractId: "LATEST-HTTP", eventType: "funded",      ledger: 30, timestamp: 3000, dataJson: JSON.stringify({ client: addr }) });
    seedEvent(testDb, { contractId: "LATEST-HTTP", eventType: "approved",    ledger: 20, timestamp: 2000, dataJson: JSON.stringify({ client: addr }) });

    const res = await request(app).get(`/api/jobs/by-wallet/${addr}`).expect(200);

    expect(res.body.jobs[0].latest_event_type).toBe("funded");
    expect(res.body.jobs[0].latest_ledger).toBe(30);
  });
});
