import { jest } from "@jest/globals";
import request from "supertest";
import express from "express";

const VALID_CONTRACT = "CDD5WKK3WT3QVKXMXTJNDIXE4T73FK6GGXDSD6UTJAH6YYZU52SQ4MUH";
const VALID_ADDRESS = "GAODBHVR63Z56MVQRBEJSYM2H5423LJ4WAPUUBOFG4JYY72S6ROKVZRX";

const mockGetAccount = jest.fn<() => Promise<unknown>>();
const mockSimulateTransaction = jest.fn<() => Promise<unknown>>();

jest.unstable_mockModule("@stellar/stellar-sdk/rpc", () => ({
  Server: class MockServer {
    getAccount = mockGetAccount;
    simulateTransaction = mockSimulateTransaction;
  },
}));

const { default: router } = await import("../src/routes/jobs.js");

function buildApp() {
  const app = express();
  app.use(express.json());
  app.use("/api/jobs", router);
  return app;
}

const ENDPOINT = `/api/jobs/${VALID_CONTRACT}/milestones/0/time-remaining`;

describe("GET /api/jobs/:contractId/milestones/:index/time-remaining", () => {
  beforeEach(() => {
    mockGetAccount.mockReset();
    mockSimulateTransaction.mockReset();

    mockGetAccount.mockResolvedValue({
      accountId: () => VALID_ADDRESS,
      sequenceNumber: () => "1",
      incrementSequenceNumber: () => {},
    });
  });

  // ── params validation ──────────────────────────────────────────────────────

  it("returns 400 for an invalid contractId", async () => {
    const res = await request(buildApp())
      .get("/api/jobs/not-a-valid-contract/milestones/0/time-remaining")
      .expect(400);

    expect(res.body).toMatchObject({ success: false, error: expect.any(String) });
  });

  it("returns 400 when contractId is a G... account address", async () => {
    const res = await request(buildApp())
      .get(`/api/jobs/${VALID_ADDRESS}/milestones/0/time-remaining`)
      .expect(400);

    expect(res.body.success).toBe(false);
    expect(res.body.error).toMatch(/valid Stellar contract address/i);
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

  it("validates before the route handler reaches Stellar RPC", async () => {
    await request(buildApp())
      .get(`/api/jobs/not-a-valid-contract/milestones/0/time-remaining`)
      .expect(400);

    expect(mockGetAccount).not.toHaveBeenCalled();
    expect(mockSimulateTransaction).not.toHaveBeenCalled();
  });

  // ── success path ──────────────────────────────────────────────────────────

  it("returns 200 with secondsRemaining on valid input", async () => {
    mockSimulateTransaction.mockResolvedValue({ result: { retval: 3600 } });

    const res = await request(buildApp()).get(ENDPOINT).expect(200);

    expect(res.body).toEqual({ success: true, secondsRemaining: 3600 });
  });

  it("calls the Stellar RPC exactly once per request", async () => {
    mockSimulateTransaction.mockResolvedValue({ result: { retval: 120 } });

    await request(buildApp()).get(ENDPOINT).expect(200);

    expect(mockGetAccount).toHaveBeenCalledTimes(1);
    expect(mockSimulateTransaction).toHaveBeenCalledTimes(1);
  });

  // ── simulation error classification ──────────────────────────────────────

  it("returns 404 when simulation reports the contract is not found", async () => {
    mockSimulateTransaction.mockResolvedValue({ error: "contract not found" });

    const res = await request(buildApp()).get(ENDPOINT).expect(404);

    expect(res.body).toEqual({
      success: false,
      error: "Contract not found on network",
    });
  });

  it("returns 422 when simulation reports a contract revert", async () => {
    mockSimulateTransaction.mockResolvedValue({ error: "contract error #101: panic" });

    const res = await request(buildApp()).get(ENDPOINT).expect(422);

    expect(res.body).toEqual({
      success: false,
      error: "Contract execution reverted (error code 101)",
    });
  });

  it("returns sanitized 500 for unexpected simulation errors (no leak)", async () => {
    mockSimulateTransaction.mockResolvedValue({
      error: "host unreachable - internal detail",
    });

    const res = await request(buildApp()).get(ENDPOINT).expect(500);

    expect(res.body).toEqual({ success: false, error: "Internal server error" });
    expect(res.text).not.toContain("host unreachable");
    expect(res.text).not.toContain("internal detail");
    expect(res.text).not.toContain("stack");
  });

  // ── unhandled exception path (try/catch wrapper, #125) ───────────────────

  it("returns sanitized 500 when getAccount throws (no leak)", async () => {
    mockGetAccount.mockRejectedValue(new Error("connection refused - detail"));

    const res = await request(buildApp()).get(ENDPOINT).expect(500);

    expect(res.body).toEqual({ success: false, error: "Internal server error" });
    expect(res.text).not.toContain("connection refused");
    expect(res.text).not.toContain("stack");
  });

  it("returns sanitized 500 when simulateTransaction rejects (no leak)", async () => {
    mockSimulateTransaction.mockRejectedValue(new Error("Database connection timeout"));

    const res = await request(buildApp()).get(ENDPOINT).expect(500);

    expect(res.body).toEqual({ success: false, error: "Internal server error" });
    expect(res.text).not.toContain("Database connection timeout");
  });

  it("never includes stack traces in 500 responses", async () => {
    const stackError = new Error("boom");
    stackError.stack = "Error: boom\n    at Object.<anonymous> (/app/src/file.ts:1:1)";
    mockGetAccount.mockRejectedValue(stackError);

    const res = await request(buildApp()).get(ENDPOINT).expect(500);

    expect(res.body).toEqual({ success: false, error: "Internal server error" });
    expect(res.text).not.toContain("/app/src");
    expect(res.text).not.toContain("file.ts");
    expect(res.text).not.toContain("at ");
  });
});
