import { jest } from "@jest/globals";
import request from "supertest";
import express from "express";

const VALID_CONTRACT = "CDD5WKK3WT3QVKXMXTJNDIXE4T73FK6GGXDSD6UTJAH6YYZU52SQ4MUH";
const VALID_ADDRESS = "GAODBHVR63Z56MVQRBEJSYM2H5423LJ4WAPUUBOFG4JYY72S6ROKVZRX";
const VALID_ADDRESS_2 = "GB2AAQ5ECB3LG5XN7VJQ5T7VBR2DXBVXA5HH24376WIFPE7PQN6HBT5X"; // second G... address

const mockGetAccount = jest.fn<() => Promise<unknown>>();
const mockPrepareTransaction = jest.fn<() => Promise<unknown>>();

jest.unstable_mockModule("@stellar/stellar-sdk/rpc", () => ({
  Server: class MockServer {
    getAccount = mockGetAccount;
    prepareTransaction = mockPrepareTransaction;
  },
}));

jest.unstable_mockModule("../src/utils/logger.js", () => ({
  default: { info: jest.fn(), warn: jest.fn(), error: jest.fn() },
}));

const { default: router, resetClaimAutoReleaseCache } = await import("../src/routes/jobs.js");
const { default: mockLogger } = await import("../src/utils/logger.js");

function buildApp() {
  const app = express();
  app.use(express.json());
  app.use("/api/jobs", router);
  return app;
}

const ENDPOINT = `/api/jobs/${VALID_CONTRACT}/milestones/0/claim-auto-release`;
const VALID_BODY = { sourceAddress: VALID_ADDRESS };

const MOCK_ACCOUNT = {
  accountId: () => VALID_ADDRESS,
  sequenceNumber: () => "1",
  incrementSequenceNumber: () => {},
};

// ---------------------------------------------------------------------------
// Params validation
// ---------------------------------------------------------------------------

describe("POST /api/jobs/:contractId/milestones/:index/claim-auto-release – params validation", () => {
  beforeEach(() => {
    resetClaimAutoReleaseCache();
    mockGetAccount.mockReset();
    mockPrepareTransaction.mockReset();
  });

  it("returns 400 for an invalid contractId", async () => {
    const res = await request(buildApp())
      .post("/api/jobs/not-a-valid-contract/milestones/0/claim-auto-release")
      .send(VALID_BODY)
      .expect(400);

    expect(res.body).toMatchObject({ success: false, error: expect.any(String) });
    expect(mockGetAccount).not.toHaveBeenCalled();
  });

  it("returns 400 when contractId is a G... account address", async () => {
    const res = await request(buildApp())
      .post(`/api/jobs/${VALID_ADDRESS}/milestones/0/claim-auto-release`)
      .send(VALID_BODY)
      .expect(400);

    expect(res.body.success).toBe(false);
    expect(res.body.error).toMatch(/valid Stellar contract address/i);
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

    expect(res.body.success).toBe(false);
  });

  it("accepts index 0 as valid", async () => {
    mockGetAccount.mockResolvedValue(MOCK_ACCOUNT);
    mockPrepareTransaction.mockResolvedValue({ toXDR: () => "AAAAAQ==" });

    const res = await request(buildApp())
      .post(`/api/jobs/${VALID_CONTRACT}/milestones/0/claim-auto-release`)
      .send(VALID_BODY)
      .expect(200);

    expect(res.body.success).toBe(true);
  });

  it("accepts a large index", async () => {
    mockGetAccount.mockResolvedValue(MOCK_ACCOUNT);
    mockPrepareTransaction.mockResolvedValue({ toXDR: () => "AAAAAQ==" });

    const res = await request(buildApp())
      .post(`/api/jobs/${VALID_CONTRACT}/milestones/99/claim-auto-release`)
      .send(VALID_BODY)
      .expect(200);

    expect(res.body.success).toBe(true);
  });

  it("400 response has exactly { success, error } keys", async () => {
    const res = await request(buildApp())
      .post("/api/jobs/bad/milestones/0/claim-auto-release")
      .send(VALID_BODY)
      .expect(400);

    expect(Object.keys(res.body)).toEqual(["success", "error"]);
  });
});

// ---------------------------------------------------------------------------
// Body validation
// ---------------------------------------------------------------------------

describe("POST /api/jobs/:contractId/milestones/:index/claim-auto-release – body validation", () => {
  beforeEach(() => {
    resetClaimAutoReleaseCache();
    mockGetAccount.mockReset();
    mockPrepareTransaction.mockReset();
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
    expect(mockGetAccount).not.toHaveBeenCalled();
  });

  it("returns 400 when sourceAddress is not a valid Stellar address", async () => {
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

  it("returns 400 when sourceAddress is a number", async () => {
    const res = await request(buildApp())
      .post(ENDPOINT)
      .send({ sourceAddress: 12345 })
      .expect(400);

    expect(res.body.success).toBe(false);
    expect(typeof res.body.error).toBe("string");
  });

  it("returns 400 when body is empty string for sourceAddress", async () => {
    const res = await request(buildApp())
      .post(ENDPOINT)
      .send({ sourceAddress: "" })
      .expect(400);

    expect(res.body.success).toBe(false);
  });

  it("validation fires before any RPC call is made", async () => {
    await request(buildApp())
      .post(ENDPOINT)
      .send({ sourceAddress: "not-a-stellar-address" })
      .expect(400);

    expect(mockGetAccount).not.toHaveBeenCalled();
    expect(mockPrepareTransaction).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// Success path
// ---------------------------------------------------------------------------

describe("POST /api/jobs/:contractId/milestones/:index/claim-auto-release – success", () => {
  beforeEach(() => {
    resetClaimAutoReleaseCache();
    mockGetAccount.mockReset();
    mockPrepareTransaction.mockReset();
    mockGetAccount.mockResolvedValue(MOCK_ACCOUNT);
  });

  it("returns 200 with xdr on valid input", async () => {
    mockPrepareTransaction.mockResolvedValue({ toXDR: () => "AAAAAQ==" });

    const res = await request(buildApp()).post(ENDPOINT).send(VALID_BODY).expect(200);

    expect(res.body).toEqual({ success: true, xdr: "AAAAAQ==" });
  });

  it("response shape is exactly { success, xdr }", async () => {
    mockPrepareTransaction.mockResolvedValue({ toXDR: () => "AAAAAQ==" });

    const res = await request(buildApp()).post(ENDPOINT).send(VALID_BODY).expect(200);

    expect(Object.keys(res.body)).toEqual(["success", "xdr"]);
    expect(res.body.success).toBe(true);
    expect(typeof res.body.xdr).toBe("string");
  });

  it("calls getAccount with sourceAddress", async () => {
    mockPrepareTransaction.mockResolvedValue({ toXDR: () => "AAAAAQ==" });

    await request(buildApp()).post(ENDPOINT).send(VALID_BODY).expect(200);

    expect(mockGetAccount).toHaveBeenCalledWith(VALID_ADDRESS);
  });
});

// ---------------------------------------------------------------------------
// Caching
// ---------------------------------------------------------------------------

describe("POST /api/jobs/:contractId/milestones/:index/claim-auto-release – caching", () => {
  beforeEach(() => {
    resetClaimAutoReleaseCache();
    mockGetAccount.mockReset();
    mockPrepareTransaction.mockReset();
    mockGetAccount.mockResolvedValue(MOCK_ACCOUNT);
  });

  it("serves the second request from cache without calling RPC again", async () => {
    mockPrepareTransaction.mockResolvedValue({ toXDR: () => "AAAAAQ==" });

    const app = buildApp();
    const first = await request(app).post(ENDPOINT).send(VALID_BODY).expect(200);
    const second = await request(app).post(ENDPOINT).send(VALID_BODY).expect(200);

    expect(first.body).toEqual({ success: true, xdr: "AAAAAQ==" });
    expect(second.body).toEqual({ success: true, xdr: "AAAAAQ==" });
    expect(mockGetAccount).toHaveBeenCalledTimes(1);
    expect(mockPrepareTransaction).toHaveBeenCalledTimes(1);
  });

  it("deduplicates concurrent requests — only one RPC call", async () => {
    mockPrepareTransaction.mockImplementation(
      () => new Promise((resolve) => setTimeout(() => resolve({ toXDR: () => "AAAAAQ==" }), 20)),
    );

    const app = buildApp();
    const results = await Promise.all([
      request(app).post(ENDPOINT).send(VALID_BODY),
      request(app).post(ENDPOINT).send(VALID_BODY),
      request(app).post(ENDPOINT).send(VALID_BODY),
    ]);

    for (const res of results) {
      expect(res.status).toBe(200);
      expect(res.body).toEqual({ success: true, xdr: "AAAAAQ==" });
    }
    expect(mockGetAccount).toHaveBeenCalledTimes(1);
    expect(mockPrepareTransaction).toHaveBeenCalledTimes(1);
  });

  it("different sourceAddress values each trigger their own RPC call", async () => {
    mockPrepareTransaction
      .mockResolvedValueOnce({ toXDR: () => "XDR_A" })
      .mockResolvedValueOnce({ toXDR: () => "XDR_B" });

    const app = buildApp();

    const resA = await request(app).post(ENDPOINT).send({ sourceAddress: VALID_ADDRESS }).expect(200);
    const resB = await request(app)
      .post(ENDPOINT)
      .send({ sourceAddress: VALID_ADDRESS_2 })
      .expect(200);

    expect(resA.body.xdr).toBe("XDR_A");
    expect(resB.body.xdr).toBe("XDR_B");
    expect(mockPrepareTransaction).toHaveBeenCalledTimes(2);
  });

  it("different milestone indexes get separate cache entries", async () => {
    mockPrepareTransaction
      .mockResolvedValueOnce({ toXDR: () => "XDR_0" })
      .mockResolvedValueOnce({ toXDR: () => "XDR_1" });

    const app = buildApp();

    const res0 = await request(app)
      .post(`/api/jobs/${VALID_CONTRACT}/milestones/0/claim-auto-release`)
      .send(VALID_BODY)
      .expect(200);
    const res1 = await request(app)
      .post(`/api/jobs/${VALID_CONTRACT}/milestones/1/claim-auto-release`)
      .send(VALID_BODY)
      .expect(200);

    expect(res0.body.xdr).toBe("XDR_0");
    expect(res1.body.xdr).toBe("XDR_1");
    expect(mockPrepareTransaction).toHaveBeenCalledTimes(2);
  });

  it("does not cache a failed request — next request retries RPC", async () => {
    mockPrepareTransaction
      .mockRejectedValueOnce(new Error("RPC error"))
      .mockResolvedValueOnce({ toXDR: () => "AAAAAQ==" });

    const app = buildApp();

    await request(app).post(ENDPOINT).send(VALID_BODY).expect(500);
    const retry = await request(app).post(ENDPOINT).send(VALID_BODY).expect(200);

    expect(retry.body).toEqual({ success: true, xdr: "AAAAAQ==" });
    expect(mockPrepareTransaction).toHaveBeenCalledTimes(2);
  });
});

// ---------------------------------------------------------------------------
// Error path
// ---------------------------------------------------------------------------

describe("POST /api/jobs/:contractId/milestones/:index/claim-auto-release – errors", () => {
  beforeEach(() => {
    resetClaimAutoReleaseCache();
    mockGetAccount.mockReset();
    mockPrepareTransaction.mockReset();
    mockGetAccount.mockResolvedValue(MOCK_ACCOUNT);
  });

  it("returns 500 when getAccount throws", async () => {
    mockGetAccount.mockRejectedValue(new Error("account not found"));

    const res = await request(buildApp()).post(ENDPOINT).send(VALID_BODY).expect(500);

    expect(res.body).toEqual({ success: false, error: "Internal server error" });
  });

  it("returns 500 when prepareTransaction throws", async () => {
    mockPrepareTransaction.mockRejectedValue(new Error("RPC failure"));

    const res = await request(buildApp()).post(ENDPOINT).send(VALID_BODY).expect(500);

    expect(res.body).toEqual({ success: false, error: "Internal server error" });
  });

  it("does not leak internal error details in the 500 response", async () => {
    mockPrepareTransaction.mockRejectedValue(new Error("secret internal detail: api-key-abc"));

    const res = await request(buildApp()).post(ENDPOINT).send(VALID_BODY).expect(500);

    expect(JSON.stringify(res.body)).not.toContain("api-key");
    expect(JSON.stringify(res.body)).not.toContain("secret");
    expect(res.body.error).toBe("Internal server error");
  });

  it("500 response has exactly { success, error } keys", async () => {
    mockPrepareTransaction.mockRejectedValue(new Error("boom"));

    const res = await request(buildApp()).post(ENDPOINT).send(VALID_BODY).expect(500);

    expect(Object.keys(res.body)).toEqual(["success", "error"]);
    expect(res.body.success).toBe(false);
  });

  it("logs the error when RPC fails", async () => {
    (mockLogger.error as ReturnType<typeof jest.fn>).mockClear();
    mockPrepareTransaction.mockRejectedValue(new Error("rpc down"));

    await request(buildApp()).post(ENDPOINT).send(VALID_BODY).expect(500);

    const errorCalls = (mockLogger.error as ReturnType<typeof jest.fn>).mock.calls.filter(
      ([m]) => m === "Failed to build claim-auto-release tx",
    );
    expect(errorCalls).toHaveLength(1);
  });
});
