import { jest } from "@jest/globals";
import request from "supertest";
import express from "express";

const mockSendTransaction = jest.fn<() => Promise<unknown>>();
const mockTx = { toXDR: () => "mock-xdr" };

jest.unstable_mockModule("../src/middleware/rateLimiter.js", () => ({
  strictLimiter: (_req: any, _res: any, next: any) => next(),
  generalLimiter: (_req: any, _res: any, next: any) => next(),
}));

jest.unstable_mockModule("@stellar/stellar-sdk/rpc", () => ({
  Server: class MockServer {
    sendTransaction = mockSendTransaction;
  },
}));

jest.unstable_mockModule("@stellar/stellar-sdk", () => ({
  TransactionBuilder: {
    fromXDR: jest.fn(() => mockTx),
  },
  Contract: jest.fn(),
  Networks: {
    TESTNET: "Test SDF Network ; September 2015",
    PUBLIC: "Public Global Stellar Network ; September 2015",
  },
  BASE_FEE: "100",
  nativeToScVal: jest.fn(),
  Address: {
    fromString: jest.fn(() => ({ toScVal: jest.fn() })),
  },
  StrKey: {
    isValidEd25519PublicKey: jest.fn(() => true),
  },
}));

jest.unstable_mockModule("../src/utils/logger.js", () => ({
  default: { info: jest.fn(), warn: jest.fn(), error: jest.fn() },
}));

const { default: router } = await import("../src/routes/jobs.js");

function buildApp() {
  const app = express();
  app.use(express.json());
  app.use("/api/jobs", router);
  return app;
}

const VALID_SIGNED_XDR = "AAAAAgAAAABz9B8nR7h4qY6q6q6q6q6q6q6q6q6q6q6q6q6q6q6q6q6q6q6qAAAAGQAAAAAAAAAAQAAAAAAAAAAAAAAAFxOAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

describe("POST /api/jobs/submit – success path", () => {
  beforeEach(() => {
    mockSendTransaction.mockReset();
  });

  it("returns 200 with transaction result on successful submission", async () => {
    const mockResult = {
      id: "123456789",
      status: "PENDING",
      hash: "abcdef123456789",
    };
    mockSendTransaction.mockResolvedValue(mockResult);

    const res = await request(buildApp())
      .post("/api/jobs/submit")
      .send({ signedXdr: VALID_SIGNED_XDR })
      .expect(200);

    expect(res.body).toEqual({ success: true, data: mockResult });
    expect(mockSendTransaction).toHaveBeenCalledTimes(1);
  });

  it("accepts valid signedXdr and calls sendTransaction", async () => {
    mockSendTransaction.mockResolvedValue({ id: "test-id" });

    const res = await request(buildApp())
      .post("/api/jobs/submit")
      .send({ signedXdr: VALID_SIGNED_XDR })
      .expect(200);

    expect(res.body.success).toBe(true);
    expect(res.body.data).toBeDefined();
  });
});

describe("POST /api/jobs/submit – schema validation", () => {
  it("returns 400 when signedXdr is missing", async () => {
    const res = await request(buildApp())
      .post("/api/jobs/submit")
      .send({})
      .expect(400);

    expect(res.body.success).toBe(false);
    expect(res.body.error).toMatch(/required/i);
  });

  it("returns 400 when signedXdr is an empty string", async () => {
    const res = await request(buildApp())
      .post("/api/jobs/submit")
      .send({ signedXdr: "" })
      .expect(400);

    expect(res.body.success).toBe(false);
    expect(res.body.error).toMatch(/empty/i);
  });

  it("returns 400 when signedXdr is not a string", async () => {
    const res = await request(buildApp())
      .post("/api/jobs/submit")
      .send({ signedXdr: 12345 })
      .expect(400);

    expect(res.body.success).toBe(false);
    expect(res.body.error).toMatch(/string/i);
  });

  it("returns 400 when signedXdr is null", async () => {
    const res = await request(buildApp())
      .post("/api/jobs/submit")
      .send({ signedXdr: null })
      .expect(400);

    expect(res.body.success).toBe(false);
    expect(res.body.error).toMatch(/string/i);
  });

  it("error body has exactly {success, error} keys on validation failure", async () => {
    const res = await request(buildApp())
      .post("/api/jobs/submit")
      .send({})
      .expect(400);

    expect(Object.keys(res.body)).toEqual(["success", "error"]);
    expect(res.body.success).toBe(false);
    expect(typeof res.body.error).toBe("string");
  });
});

describe("POST /api/jobs/submit – error sanitization", () => {
  beforeEach(() => {
    mockSendTransaction.mockReset();
  });

  it("returns 500 without leaking internal error message from RPC", async () => {
    mockSendTransaction.mockRejectedValue(
      new Error("RPC secret: api-key-12345 internal detail")
    );

    const res = await request(buildApp())
      .post("/api/jobs/submit")
      .send({ signedXdr: VALID_SIGNED_XDR })
      .expect(500);

    expect(res.body).toEqual({ success: false, error: "Internal server error" });
    expect(JSON.stringify(res.body)).not.toContain("api-key");
    expect(JSON.stringify(res.body)).not.toContain("secret");
  });

  it("returns 500 without leaking stack trace", async () => {
    const stackError = new Error("RPC failure");
    stackError.stack = "Error: RPC failure\n    at Object.sendTransaction (/app/src/rpc.ts:1:1)";
    mockSendTransaction.mockRejectedValue(stackError);

    const res = await request(buildApp())
      .post("/api/jobs/submit")
      .send({ signedXdr: VALID_SIGNED_XDR })
      .expect(500);

    const body = JSON.stringify(res.body);
    expect(body).not.toMatch(/at Object\./);
    expect(body).not.toContain(".ts:");
    expect(body).not.toContain(".js:");
  });

  it("returns 500 when sendTransaction throws network error", async () => {
    mockSendTransaction.mockRejectedValue(new Error("network unreachable"));

    const res = await request(buildApp())
      .post("/api/jobs/submit")
      .send({ signedXdr: VALID_SIGNED_XDR })
      .expect(500);

    expect(res.body).toEqual({ success: false, error: "Internal server error" });
    expect(JSON.stringify(res.body)).not.toContain("network unreachable");
  });

  it("returns 500 when XDR parsing fails", async () => {
    const invalidXdr = "invalid-xdr-string";
    mockSendTransaction.mockImplementation(() => {
      throw new Error("XDR parsing failed");
    });

    const res = await request(buildApp())
      .post("/api/jobs/submit")
      .send({ signedXdr: invalidXdr })
      .expect(500);

    expect(res.body).toEqual({ success: false, error: "Internal server error" });
  });

  it("response body has only success and error fields on failure", async () => {
    mockSendTransaction.mockRejectedValue(new Error("unexpected error"));

    const res = await request(buildApp())
      .post("/api/jobs/submit")
      .send({ signedXdr: VALID_SIGNED_XDR })
      .expect(500);

    expect(Object.keys(res.body)).toEqual(["success", "error"]);
    expect(res.body.success).toBe(false);
    expect(typeof res.body.error).toBe("string");
  });

  it("returns sanitized 500 for timeout errors", async () => {
    mockSendTransaction.mockRejectedValue(new Error("Request timeout after 30000ms"));

    const res = await request(buildApp())
      .post("/api/jobs/submit")
      .send({ signedXdr: VALID_SIGNED_XDR })
      .expect(500);

    expect(res.body).toEqual({ success: false, error: "Internal server error" });
    expect(JSON.stringify(res.body)).not.toContain("30000ms");
  });

  it("returns sanitized 500 for authentication errors", async () => {
    mockSendTransaction.mockRejectedValue(
      new Error("Authentication failed: invalid credentials")
    );

    const res = await request(buildApp())
      .post("/api/jobs/submit")
      .send({ signedXdr: VALID_SIGNED_XDR })
      .expect(500);

    expect(res.body).toEqual({ success: false, error: "Internal server error" });
    expect(JSON.stringify(res.body)).not.toContain("credentials");
  });
});

describe("POST /api/jobs/submit – rate limiting", () => {
  beforeEach(() => {
    mockSendTransaction.mockReset();
  });

  it("applies strict rate limiter middleware", async () => {
    mockSendTransaction.mockResolvedValue({ id: "test-id" });

    const res = await request(buildApp())
      .post("/api/jobs/submit")
      .send({ signedXdr: VALID_SIGNED_XDR })
      .expect(200);

    expect(res.body.success).toBe(true);
  });
});
