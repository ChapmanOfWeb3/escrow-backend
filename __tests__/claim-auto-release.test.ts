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

  it("returns 400 for a negative index", async () => {
    const res = await request(buildApp())
      .post(`/api/jobs/${VALID_CONTRACT}/milestones/-1/claim-auto-release`)
      .send(VALID_BODY)
      .expect(400);

    expect(res.body).toEqual({
      success: false,
      error: "index must be a non-negative integer",
    });
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

  it("returns 400 when the request body contains extra fields", async () => {
    const res = await request(buildApp())
      .post(ENDPOINT)
      .send({ sourceAddress: VALID_ADDRESS, extraField: "not-allowed" })
      .expect(400);

    expect(res.body.success).toBe(false);
    expect(res.body.error).toMatch(/unrecognized key/i);
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
