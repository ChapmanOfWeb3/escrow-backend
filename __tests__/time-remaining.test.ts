import { jest } from "@jest/globals";
import request from "supertest";
import express from "express";

const VALID_CONTRACT = "CDD5WKK3WT3QVKXMXTJNDIXE4T73FK6GGXDSD6UTJAH6YYZU52SQ4MUH";
const SECOND_CONTRACT = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC";

const mockGetAccount = jest.fn<() => Promise<unknown>>();
const mockSimulateTransaction = jest.fn<() => Promise<unknown>>();

const mockLoggerInfo = jest.fn();
const mockLoggerWarn = jest.fn();
const mockLoggerError = jest.fn();

jest.unstable_mockModule("../src/utils/logger.js", () => ({
  default: {
    info: mockLoggerInfo,
    warn: mockLoggerWarn,
    error: mockLoggerError,
  },
}));

jest.unstable_mockModule("@stellar/stellar-sdk/rpc", () => ({
  Server: class MockServer {
    getAccount = mockGetAccount;
    simulateTransaction = mockSimulateTransaction;
  },
}));

const { default: router, resetTimeRemainingCache } = await import("../src/routes/jobs.js");

function buildApp() {
  const app = express();
  app.use(express.json());
  app.use("/api/jobs", router);
  return app;
}

const ENDPOINT = `/api/jobs/${VALID_CONTRACT}/milestones/0/time-remaining`;

describe("GET /api/jobs/:contractId/milestones/:index/time-remaining", () => {
  const originalApiKey = process.env.API_KEY;

  beforeEach(() => {
    resetTimeRemainingCache();
    mockGetAccount.mockReset();
    mockSimulateTransaction.mockReset();
    mockLoggerInfo.mockReset();
    mockLoggerWarn.mockReset();
    mockLoggerError.mockReset();
    delete process.env.API_KEY;

    mockGetAccount.mockResolvedValue({
      accountId: () => "GAODBHVR63Z56MVQRBEJSYM2H5423LJ4WAPUUBOFG4JYY72S6ROKVZRX",
      sequenceNumber: () => "123456789",
      incrementSequenceNumber: () => {},
    });
  });

  afterAll(() => {
    if (originalApiKey === undefined) {
      delete process.env.API_KEY;
    } else {
      process.env.API_KEY = originalApiKey;
    }
  });

  // --- Request validation ---
  describe("Params validation", () => {
    it("returns 400 for an invalid contractId", async () => {
      const res = await request(buildApp())
        .get("/api/jobs/not-a-valid-contract/milestones/0/time-remaining")
        .expect(400);

      expect(res.body).toEqual({
        success: false,
        error: "contractId must be a valid Stellar contract address (C...)",
      });
      expect(mockGetAccount).not.toHaveBeenCalled();
    });

    it("returns 400 for a non-numeric index", async () => {
      const res = await request(buildApp())
        .get(`/api/jobs/${VALID_CONTRACT}/milestones/abc/time-remaining`)
        .expect(400);

      expect(res.body).toEqual({
        success: false,
        error: "index must be a non-negative integer",
      });
    });

    it("returns 400 for a decimal index", async () => {
      const res = await request(buildApp())
        .get(`/api/jobs/${VALID_CONTRACT}/milestones/1.5/time-remaining`)
        .expect(400);

      expect(res.body.success).toBe(false);
      expect(res.body.error).toMatch(/index/i);
    });

    it("returns 400 for a negative index", async () => {
      const res = await request(buildApp())
        .get(`/api/jobs/${VALID_CONTRACT}/milestones/-1/time-remaining`)
        .expect(400);

      expect(res.body.success).toBe(false);
    });
  });

  // --- Success path & response envelope ---
  describe("Success response", () => {
    it("returns 200 with a standardized {success, data} envelope", async () => {
      mockSimulateTransaction.mockResolvedValue({ result: { retval: 86400 } });

      const res = await request(buildApp()).get(ENDPOINT).expect(200);

      expect(res.body).toEqual({ success: true, data: { secondsRemaining: 86400 } });
    });

    it("response body has exactly {success, data} keys on success", async () => {
      mockSimulateTransaction.mockResolvedValue({ result: { retval: 42 } });

      const res = await request(buildApp()).get(ENDPOINT).expect(200);

      expect(Object.keys(res.body)).toEqual(["success", "data"]);
      expect(Object.keys(res.body.data)).toEqual(["secondsRemaining"]);
    });
  });

  // --- Accurate HTTP status codes ---
  describe("Standard error status codes", () => {
    it("returns 401 when API_KEY is configured and the header is missing", async () => {
      process.env.API_KEY = "secret-key";

      const res = await request(buildApp()).get(ENDPOINT).expect(401);

      expect(res.body).toEqual({ success: false, error: "Unauthorized" });
      expect(mockGetAccount).not.toHaveBeenCalled();
    });

    it("returns 401 when API_KEY is configured and the header is wrong", async () => {
      process.env.API_KEY = "secret-key";

      const res = await request(buildApp())
        .get(ENDPOINT)
        .set("x-api-key", "wrong-key")
        .expect(401);

      expect(res.body).toEqual({ success: false, error: "Unauthorized" });
    });

    it("returns 200 when API_KEY is configured and the header matches", async () => {
      process.env.API_KEY = "secret-key";
      mockSimulateTransaction.mockResolvedValue({ result: { retval: 100 } });

      const res = await request(buildApp())
        .get(ENDPOINT)
        .set("x-api-key", "secret-key")
        .expect(200);

      expect(res.body).toEqual({ success: true, data: { secondsRemaining: 100 } });
    });

    it("returns 404 when simulation reports the contract/job was not found", async () => {
      mockSimulateTransaction.mockResolvedValue({
        error: "contract not found on network",
      });

      const res = await request(buildApp()).get(ENDPOINT).expect(404);

      expect(res.body).toEqual({ success: false, error: "Job not found" });
    });

    it("returns 422 when the contract execution reverts", async () => {
      mockSimulateTransaction.mockResolvedValue({ error: "contract error #5" });

      const res = await request(buildApp()).get(ENDPOINT).expect(422);

      expect(res.body).toEqual({
        success: false,
        error: "Contract execution reverted (error code 5)",
      });
    });

    it("returns 500 for unexpected simulation failures", async () => {
      mockSimulateTransaction.mockResolvedValue({ error: "host unreachable" });

      const res = await request(buildApp()).get(ENDPOINT).expect(500);

      expect(res.body).toEqual({ success: false, error: "Internal server error" });
    });

    it("returns 500 when retval is missing from a successful simulation", async () => {
      mockSimulateTransaction.mockResolvedValue({ result: {} });

      const res = await request(buildApp()).get(ENDPOINT).expect(500);

      expect(res.body).toEqual({ success: false, error: "Internal server error" });
    });

    it("returns 500 without leaking the thrown exception message", async () => {
      mockGetAccount.mockRejectedValue(
        new Error("DB secret: postgres://admin:password@db/prod"),
      );

      const res = await request(buildApp()).get(ENDPOINT).expect(500);

      expect(res.body).toEqual({ success: false, error: "Internal server error" });
      expect(JSON.stringify(res.body)).not.toContain("postgres");
      expect(JSON.stringify(res.body)).not.toContain("password");
    });

    it("response body has exactly {success, error} keys on failure", async () => {
      mockSimulateTransaction.mockResolvedValue({ error: "host unreachable" });

      const res = await request(buildApp()).get(ENDPOINT).expect(500);

      expect(Object.keys(res.body)).toEqual(["success", "error"]);
      expect(res.body.success).toBe(false);
      expect(typeof res.body.error).toBe("string");
    });
  });

  // --- In-memory caching (duplicate network hits) ---
  describe("In-memory caching", () => {
    it("returns the value from RPC on the first request", async () => {
      mockSimulateTransaction.mockResolvedValue({ result: { retval: 500 } });

      const res = await request(buildApp()).get(ENDPOINT).expect(200);

      expect(res.body.data.secondsRemaining).toBe(500);
      expect(mockSimulateTransaction).toHaveBeenCalledTimes(1);
    });

    it("serves a second identical request from cache without calling RPC again", async () => {
      mockSimulateTransaction.mockResolvedValue({ result: { retval: 500 } });

      const app = buildApp();
      await request(app).get(ENDPOINT).expect(200);
      const cached = await request(app).get(ENDPOINT).expect(200);

      expect(cached.body).toEqual({ success: true, data: { secondsRemaining: 500 } });
      expect(mockSimulateTransaction).toHaveBeenCalledTimes(1);
    });

    it("deduplicates concurrent requests into a single RPC round-trip", async () => {
      mockSimulateTransaction.mockResolvedValue({ result: { retval: 720 } });

      const app = buildApp();
      const results = await Promise.all([
        request(app).get(ENDPOINT),
        request(app).get(ENDPOINT),
        request(app).get(ENDPOINT),
      ]);

      for (const res of results) {
        expect(res.status).toBe(200);
        expect(res.body).toEqual({ success: true, data: { secondsRemaining: 720 } });
      }
      expect(mockGetAccount).toHaveBeenCalledTimes(1);
      expect(mockSimulateTransaction).toHaveBeenCalledTimes(1);
    });

    it("caches different contractId/index pairs independently", async () => {
      mockSimulateTransaction
        .mockResolvedValueOnce({ result: { retval: 111 } })
        .mockResolvedValueOnce({ result: { retval: 222 } })
        .mockResolvedValueOnce({ result: { retval: 333 } });

      const app = buildApp();
      const r1 = await request(app).get(ENDPOINT).expect(200);
      const r2 = await request(app)
        .get(`/api/jobs/${SECOND_CONTRACT}/milestones/0/time-remaining`)
        .expect(200);
      const r3 = await request(app)
        .get(`/api/jobs/${VALID_CONTRACT}/milestones/1/time-remaining`)
        .expect(200);

      expect(r1.body.data.secondsRemaining).toBe(111);
      expect(r2.body.data.secondsRemaining).toBe(222);
      expect(r3.body.data.secondsRemaining).toBe(333);
      expect(mockSimulateTransaction).toHaveBeenCalledTimes(3);
    });

    it("does not cache an error response, so the next request retries the RPC", async () => {
      mockSimulateTransaction
        .mockResolvedValueOnce({ error: "host unreachable" })
        .mockResolvedValueOnce({ result: { retval: 999 } });

      const app = buildApp();
      const first = await request(app).get(ENDPOINT).expect(500);
      expect(first.body).toEqual({ success: false, error: "Internal server error" });

      const second = await request(app).get(ENDPOINT).expect(200);
      expect(second.body).toEqual({ success: true, data: { secondsRemaining: 999 } });

      expect(mockSimulateTransaction).toHaveBeenCalledTimes(2);
    });
  });
});
