import { jest } from "@jest/globals";
import request from "supertest";
import express from "express";

const VALID_CONTRACT = "CDD5WKK3WT3QVKXMXTJNDIXE4T73FK6GGXDSD6UTJAH6YYZU52SQ4MUH";
const VALID_ADDRESS = "GAODBHVR63Z56MVQRBEJSYM2H5423LJ4WAPUUBOFG4JYY72S6ROKVZRX";

const mockGetAccount = jest.fn<() => Promise<unknown>>();
const mockPrepareTransaction = jest.fn<() => Promise<unknown>>();

jest.unstable_mockModule("@stellar/stellar-sdk/rpc", () => ({
  Server: class MockServer {
    getAccount = mockGetAccount;
    prepareTransaction = mockPrepareTransaction;
  },
}));

const { default: router, resetClaimAutoReleaseCache } = await import("../src/routes/jobs.js");

function buildApp() {
  const app = express();
  app.use(express.json());
  app.use("/api/jobs", router);
  return app;
}

const ENDPOINT = `/api/jobs/${VALID_CONTRACT}/milestones/0/claim-auto-release`;
const VALID_BODY = { sourceAddress: VALID_ADDRESS };

describe("POST /api/jobs/:contractId/milestones/:index/claim-auto-release", () => {
  beforeEach(() => {
    delete process.env.API_KEY;
    resetClaimAutoReleaseCache();
    mockGetAccount.mockReset();
    mockPrepareTransaction.mockReset();

    mockGetAccount.mockResolvedValue({
      accountId: () => VALID_ADDRESS,
      sequenceNumber: () => "1",
      incrementSequenceNumber: () => {},
    });
  });

  it("returns 400 for an invalid contractId", async () => {
    const res = await request(buildApp())
      .post("/api/jobs/not-a-valid-contract/milestones/0/claim-auto-release")
      .send(VALID_BODY)
      .expect(400);

    expect(res.body).toMatchObject({ success: false, error: expect.any(String) });
  });

  it("returns 400 for a non-numeric index", async () => {
    const res = await request(buildApp())
      .post(`/api/jobs/${VALID_CONTRACT}/milestones/abc/claim-auto-release`)
      .send(VALID_BODY)
      .expect(400);

    expect(res.body).toEqual({
      success: false,
      error: "index must be a non-negative integer",
    });
  });

  it("returns 400 for a decimal index", async () => {
    const res = await request(buildApp())
      .post(`/api/jobs/${VALID_CONTRACT}/milestones/1.5/claim-auto-release`)
      .send(VALID_BODY)
      .expect(400);

    expect(res.body.success).toBe(false);
    expect(res.body.error).toMatch(/index/i);
  });

  it("returns 400 when sourceAddress is missing", async () => {
    const res = await request(buildApp())
      .post(ENDPOINT)
      .send({})
      .expect(400);

    expect(res.body).toEqual({
      success: false,
      error: "sourceAddress is required",
    });
  });

  it("returns 400 when sourceAddress is not a valid Stellar account address", async () => {
    const res = await request(buildApp())
      .post(ENDPOINT)
      .send({ sourceAddress: "not-a-stellar-address" })
      .expect(400);

    expect(res.body).toEqual({
      success: false,
      error: "sourceAddress must be a valid Stellar account address (G...)",
    });
  });

  it("returns 400 when sourceAddress is a contract address (C...)", async () => {
    const res = await request(buildApp())
      .post(ENDPOINT)
      .send({ sourceAddress: VALID_CONTRACT })
      .expect(400);

    expect(res.body.success).toBe(false);
    expect(res.body.error).toMatch(/sourceAddress/i);
  });

  it("returns 401 when API_KEY is configured but no key provided", async () => {
    process.env.API_KEY = "test-secret-key";

    const res = await request(buildApp())
      .post(ENDPOINT)
      .send(VALID_BODY)
      .expect(401);

    expect(res.body).toEqual({ success: false, error: "Unauthorized" });
    expect(mockGetAccount).not.toHaveBeenCalled();
  });

  it("returns 401 when API_KEY is configured but wrong key provided", async () => {
    process.env.API_KEY = "test-secret-key";

    const res = await request(buildApp())
      .post(ENDPOINT)
      .set("x-api-key", "wrong-key")
      .send(VALID_BODY)
      .expect(401);

    expect(res.body).toEqual({ success: false, error: "Unauthorized" });
    expect(mockGetAccount).not.toHaveBeenCalled();
  });

  it("returns 200 when API_KEY is configured and correct key provided", async () => {
    process.env.API_KEY = "test-secret-key";
    mockPrepareTransaction.mockResolvedValue({ toXDR: () => "AAAAAQ==" });

    const res = await request(buildApp())
      .post(ENDPOINT)
      .set("x-api-key", "test-secret-key")
      .send(VALID_BODY)
      .expect(200);

    expect(res.body).toEqual({ success: true, xdr: "AAAAAQ==" });
  });

  it("returns 404 when source account is not found on the network", async () => {
    mockGetAccount.mockRejectedValue(new Error("account not found: G..."));

    const res = await request(buildApp())
      .post(ENDPOINT)
      .send(VALID_BODY)
      .expect(404);

    expect(res.body).toEqual({
      success: false,
      error: "Source account not found on network",
    });
  });

  it("returns 404 when contract is not found via prepareTransaction error", async () => {
    mockPrepareTransaction.mockRejectedValue(new Error("contract not found on network"));

    const res = await request(buildApp())
      .post(ENDPOINT)
      .send(VALID_BODY)
      .expect(404);

    expect(res.body).toEqual({
      success: false,
      error: "Contract not found on network",
    });
  });

  it("returns 404 for NotFound variant with capitalization", async () => {
    mockPrepareTransaction.mockRejectedValue(new Error("NotFound: contract does not exist"));

    const res = await request(buildApp())
      .post(ENDPOINT)
      .send(VALID_BODY)
      .expect(404);

    expect(res.body.success).toBe(false);
    expect(res.body.error).toBe("Contract not found on network");
  });

  it("returns 422 when contract execution reverts with error code", async () => {
    mockPrepareTransaction.mockRejectedValue(
      new Error("transaction simulation failed: contract error #5")
    );

    const res = await request(buildApp())
      .post(ENDPOINT)
      .send(VALID_BODY)
      .expect(422);

    expect(res.body).toEqual({
      success: false,
      error: "Contract execution reverted (error code 5)",
    });
  });

  it("returns 422 when contract execution reverts with revert/assert message", async () => {
    mockPrepareTransaction.mockRejectedValue(
      new Error("simulation failed: panic: assertion failed")
    );

    const res = await request(buildApp())
      .post(ENDPOINT)
      .send(VALID_BODY)
      .expect(422);

    expect(res.body).toEqual({
      success: false,
      error: "Contract execution reverted",
    });
  });

  it("returns 500 for unexpected internal errors", async () => {
    mockPrepareTransaction.mockRejectedValue(new Error("something went very wrong"));

    const res = await request(buildApp())
      .post(ENDPOINT)
      .send(VALID_BODY)
      .expect(500);

    expect(res.body).toEqual({
      success: false,
      error: "Internal server error",
    });
  });

  it("returns 500 and hides raw error message for network/RPC failures", async () => {
    mockGetAccount.mockRejectedValue(new Error("ECONNREFUSED connection timeout to rpc"));

    const res = await request(buildApp())
      .post(ENDPOINT)
      .send(VALID_BODY)
      .expect(500);

    expect(res.body).toEqual({
      success: false,
      error: "Internal server error",
    });
    expect(res.body.error).not.toMatch(/ECONNREFUSED/);
  });

  it("returns 200 with XDR on valid input", async () => {
    mockPrepareTransaction.mockResolvedValue({ toXDR: () => "AAAAAQ==" });

    const res = await request(buildApp()).post(ENDPOINT).send(VALID_BODY).expect(200);

    expect(res.body).toEqual({ success: true, xdr: "AAAAAQ==" });
  });

  it("serves subsequent requests from the in-memory cache without calling Stellar RPC again", async () => {
    mockPrepareTransaction.mockResolvedValue({ toXDR: () => "AAAAAQ==" });

    const app = buildApp();
    await request(app).post(ENDPOINT).send(VALID_BODY).expect(200);
    await request(app).post(ENDPOINT).send(VALID_BODY).expect(200);

    expect(mockGetAccount).toHaveBeenCalledTimes(1);
    expect(mockPrepareTransaction).toHaveBeenCalledTimes(1);
  });

  it("deduplicates concurrent requests with one Stellar RPC call", async () => {
    mockPrepareTransaction.mockResolvedValue({ toXDR: () => "AAAAAQ==" });

    const app = buildApp();
    const requests = await Promise.all([
      request(app).post(ENDPOINT).send(VALID_BODY),
      request(app).post(ENDPOINT).send(VALID_BODY),
    ]);

    expect(requests[0].status).toBe(200);
    expect(requests[1].status).toBe(200);
    expect(requests[0].body).toEqual({ success: true, xdr: "AAAAAQ==" });
    expect(requests[1].body).toEqual({ success: true, xdr: "AAAAAQ==" });
    expect(mockGetAccount).toHaveBeenCalledTimes(1);
    expect(mockPrepareTransaction).toHaveBeenCalledTimes(1);
  });

  it("validates before the route handler reaches Stellar RPC", async () => {
    await request(buildApp())
      .post(ENDPOINT)
      .send({ sourceAddress: "not-a-stellar-address" })
      .expect(400);

    expect(mockGetAccount).not.toHaveBeenCalled();
    expect(mockPrepareTransaction).not.toHaveBeenCalled();
  });
});
