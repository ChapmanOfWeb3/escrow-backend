import { jest } from "@jest/globals";
import request from "supertest";
import express from "express";

const mockGetAccount = jest.fn<() => Promise<unknown>>();
const mockPrepareTransaction = jest.fn<() => Promise<unknown>>();

jest.unstable_mockModule("../src/middleware/rateLimiter.js", () => ({
  strictLimiter: (_req: any, _res: any, next: any) => next(),
  generalLimiter: (_req: any, _res: any, next: any) => next(),
}));

jest.unstable_mockModule("@stellar/stellar-sdk/rpc", () => ({
  Server: class MockServer {
    getAccount = mockGetAccount;
    prepareTransaction = mockPrepareTransaction;
  },
}));

jest.unstable_mockModule("../src/utils/logger.js", () => ({
  default: { info: jest.fn(), warn: jest.fn(), error: jest.fn() },
}));

const { default: router } = await import("../src/routes/jobs.js");

const app = express();
app.use(express.json());
app.use("/api/jobs", router);

const VALID_BODY = {
  contractId: "CDD5WKK3WT3QVKXMXTJNDIXE4T73FK6GGXDSD6UTJAH6YYZU52SQ4MUH",
  method: "fund_job",
  args: [],
  sourceAddress: "GAODBHVR63Z56MVQRBEJSYM2H5423LJ4WAPUUBOFG4JYY72S6ROKVZRX",
};

describe("POST /api/jobs/build-tx — error sanitization (#70)", () => {
  beforeEach(() => {
    mockGetAccount.mockReset();
    mockPrepareTransaction.mockReset();

    mockGetAccount.mockResolvedValue({
      accountId: () => VALID_BODY.sourceAddress,
      sequenceNumber: () => "1",
      incrementSequenceNumber: () => {},
    });
  });

  it("returns 200 with xdr on success", async () => {
    mockPrepareTransaction.mockResolvedValue({ toXDR: () => "AAAAAQ==" });

    const res = await request(app).post("/api/jobs/build-tx").send(VALID_BODY).expect(200);
    expect(res.body).toEqual({ success: true, xdr: "AAAAAQ==" });
  });

  it("returns 500 without leaking internal error message", async () => {
    mockPrepareTransaction.mockRejectedValue(
      new Error("DB secret: postgres://admin:password@db/prod")
    );

    const res = await request(app).post("/api/jobs/build-tx").send(VALID_BODY).expect(500);

    expect(res.body).toEqual({ success: false, error: "Internal server error" });
    expect(JSON.stringify(res.body)).not.toContain("postgres");
    expect(JSON.stringify(res.body)).not.toContain("password");
  });

  it("returns 500 without leaking stack trace", async () => {
    mockPrepareTransaction.mockRejectedValue(new Error("some rpc failure"));

    const res = await request(app).post("/api/jobs/build-tx").send(VALID_BODY).expect(500);

    const body = JSON.stringify(res.body);
    expect(body).not.toMatch(/at Object\./);
    expect(body).not.toContain(".ts:");
    expect(body).not.toContain(".js:");
  });

  it("returns 500 when getAccount throws", async () => {
    mockGetAccount.mockRejectedValue(new Error("account not found: internal details"));

    const res = await request(app).post("/api/jobs/build-tx").send(VALID_BODY).expect(500);

    expect(res.body).toEqual({ success: false, error: "Internal server error" });
    expect(JSON.stringify(res.body)).not.toContain("account not found");
  });

  it("response body has only success and error fields on failure", async () => {
    mockPrepareTransaction.mockRejectedValue(new Error("unexpected"));

    const res = await request(app).post("/api/jobs/build-tx").send(VALID_BODY).expect(500);

    expect(Object.keys(res.body)).toEqual(["success", "error"]);
    expect(res.body.success).toBe(false);
    expect(typeof res.body.error).toBe("string");
  });
});

describe("POST /api/jobs/build-tx — validation", () => {
  it("returns 400 with field errors when payload is completely empty", async () => {
    const res = await request(app).post("/api/jobs/build-tx").send({}).expect(400);
    expect(res.body.success).toBe(false);
    expect(res.body.fields).toBeDefined();
    expect(res.body.fields.contractId).toMatch(/contractId is required/i);
    expect(res.body.fields.method).toMatch(/method is required/i);
    expect(res.body.fields.sourceAddress).toMatch(/address is required/i);
  });

  it("returns 400 with field errors for invalid formats", async () => {
    const res = await request(app).post("/api/jobs/build-tx").send({
      contractId: "invalid-contract-id",
      method: "",
      sourceAddress: "invalid-source-address",
    }).expect(400);
    expect(res.body.success).toBe(false);
    expect(res.body.fields).toBeDefined();
    expect(res.body.fields.contractId).toMatch(/valid Stellar contract address/i);
    expect(res.body.fields.method).toMatch(/method cannot be empty/i);
    expect(res.body.fields.sourceAddress).toMatch(/valid Stellar account address/i);
  });

  it("returns 400 if method is missing", async () => {
    const { method, ...bodyWithoutMethod } = VALID_BODY;
    const res = await request(app).post("/api/jobs/build-tx").send(bodyWithoutMethod).expect(400);
    expect(res.body.success).toBe(false);
    expect(res.body.fields).toBeDefined();
    expect(res.body.fields.method).toMatch(/method is required/i);
describe("POST /api/jobs/build-tx — request payload schema validation (#101)", () => {
  it("returns 400 when contractId is missing", async () => {
    const body = { ...VALID_BODY, contractId: undefined };
    const res = await request(app).post("/api/jobs/build-tx").send(body).expect(400);
    expect(res.body).toEqual({
      success: false,
      error: "contractId is required",
    });
  });

  it("returns 400 when contractId is not a valid Stellar contract address", async () => {
    const body = { ...VALID_BODY, contractId: "not-a-valid-contract-id" };
    const res = await request(app).post("/api/jobs/build-tx").send(body).expect(400);
    expect(res.body).toEqual({
      success: false,
      error: "contractId must be a valid Stellar contract address (C...)",
    });
  });

  it("returns 400 when method is missing", async () => {
    const body = { ...VALID_BODY, method: undefined };
    const res = await request(app).post("/api/jobs/build-tx").send(body).expect(400);
    expect(res.body).toEqual({
      success: false,
      error: "method is required",
    });
  });

  it("returns 400 when method is empty string", async () => {
    const body = { ...VALID_BODY, method: "" };
    const res = await request(app).post("/api/jobs/build-tx").send(body).expect(400);
    expect(res.body).toEqual({
      success: false,
      error: "method cannot be empty",
    });
  });

  it("returns 400 when sourceAddress is missing", async () => {
    const body = { ...VALID_BODY, sourceAddress: undefined };
    const res = await request(app).post("/api/jobs/build-tx").send(body).expect(400);
    expect(res.body).toEqual({
      success: false,
      error: "address is required",
    });
  });

  it("returns 400 when sourceAddress is not a valid Stellar account address", async () => {
    const body = { ...VALID_BODY, sourceAddress: "not-a-valid-address" };
    const res = await request(app).post("/api/jobs/build-tx").send(body).expect(400);
    expect(res.body).toEqual({
      success: false,
      error: "address must be a valid Stellar account address (G…, 56 chars)",
    });
  });

  it("returns 400 when args is not an array", async () => {
    const body = { ...VALID_BODY, args: "not-an-array" };
    const res = await request(app).post("/api/jobs/build-tx").send(body).expect(400);
    expect(res.body.success).toBe(false);
    expect(res.body.error).toContain("Expected array, received string");
  });

  it("returns 400 when contractId is invalid data type (number)", async () => {
    const body = { ...VALID_BODY, contractId: 12345 };
    const res = await request(app).post("/api/jobs/build-tx").send(body as any).expect(400);
    expect(res.body.success).toBe(false);
    expect(res.body.error).toContain("Expected string, received number");
  });

  it("returns 400 when method is invalid data type (boolean)", async () => {
    const body = { ...VALID_BODY, method: true };
    const res = await request(app).post("/api/jobs/build-tx").send(body as any).expect(400);
    expect(res.body.success).toBe(false);
    expect(res.body.error).toContain("Expected string, received boolean");
  });
});
