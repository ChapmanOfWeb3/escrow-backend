import { jest } from "@jest/globals";
import request from "supertest";
import express from "express";
import { TransactionBuilder } from "@stellar/stellar-sdk";

const mockSendTransaction = jest.fn<() => Promise<unknown>>();

jest.unstable_mockModule("@stellar/stellar-sdk/rpc", () => ({
  Server: class MockServer {
    sendTransaction = mockSendTransaction;
  },
}));

jest.unstable_mockModule("../src/utils/logger.js", () => ({
  default: { info: jest.fn(), warn: jest.fn(), error: jest.fn() },
}));

jest.unstable_mockModule("../src/middleware/rateLimiter.js", () => ({
  strictLimiter: (_req: any, _res: any, next: any) => next(),
  generalLimiter: (_req: any, _res: any, next: any) => next(),
}));

const { default: router } = await import("../src/routes/jobs.js");

const app = express();
app.use(express.json());
app.use("/api/jobs", router);

describe("POST /api/jobs/submit — error sanitization", () => {
  let fromXdrSpy: any;

  beforeEach(() => {
    mockSendTransaction.mockReset();
    if (fromXdrSpy) {
      fromXdrSpy.mockRestore();
    }
    fromXdrSpy = jest.spyOn(TransactionBuilder, "fromXDR");
  });

  afterEach(() => {
    if (fromXdrSpy) {
      fromXdrSpy.mockRestore();
    }
  });

  const VALID_BODY = {
    signedXdr: "AAAAAQ==",
  };

  it("returns 200 with data on success", async () => {
    fromXdrSpy.mockReturnValue({} as any);
    mockSendTransaction.mockResolvedValue({ hash: "tx-hash-123" });

    const res = await request(app).post("/api/jobs/submit").send(VALID_BODY).expect(200);
    expect(res.body).toEqual({ success: true, data: { hash: "tx-hash-123" } });
  });

  it("returns 500 without leaking internal error message from fromXDR", async () => {
    fromXdrSpy.mockImplementation(() => {
      throw new Error("Internal secret: invalid signature format on node xyz");
    });

    const res = await request(app).post("/api/jobs/submit").send(VALID_BODY).expect(500);

    expect(res.body).toEqual({ success: false, error: "Internal server error" });
    expect(JSON.stringify(res.body)).not.toContain("invalid signature format");
  });

  it("returns 500 without leaking stack trace from sendTransaction", async () => {
    fromXdrSpy.mockReturnValue({} as any);
    mockSendTransaction.mockRejectedValue(new Error("RPC timeout: host unreachable"));

    const res = await request(app).post("/api/jobs/submit").send(VALID_BODY).expect(500);

    expect(res.body).toEqual({ success: false, error: "Internal server error" });
    
    const bodyStr = JSON.stringify(res.body);
    expect(bodyStr).not.toMatch(/at Object\./);
    expect(bodyStr).not.toContain(".ts:");
    expect(bodyStr).not.toContain("host unreachable");
  });

  it("response body has only success and error fields on failure", async () => {
    fromXdrSpy.mockReturnValue({} as any);
    mockSendTransaction.mockRejectedValue(new Error("unexpected"));

    const res = await request(app).post("/api/jobs/submit").send(VALID_BODY).expect(500);

    expect(Object.keys(res.body)).toEqual(["success", "error"]);
    expect(res.body.success).toBe(false);
    expect(typeof res.body.error).toBe("string");
  });
});

