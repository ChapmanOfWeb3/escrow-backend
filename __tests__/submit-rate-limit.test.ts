import request from "supertest";
import express from "express";
import { jest } from "@jest/globals";

const mockSendTransaction = jest.fn<() => Promise<unknown>>();
const mockTx = { toXDR: () => "mock-xdr" };

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
const { resetSubmitRateLimitBuckets } = await import("../src/middleware/job-contract-rate-limit.js");

const VALID_SIGNED_XDR =
  "AAAAAgAAAABz9B8nR7h4qY6Ran5PlacgCUxOFxOdIQAAAAAAAAAAABAAAAAAAAAAAA==";

function buildApp() {
  const app = express();
  app.use(express.json());
  app.use("/api/jobs", router);
  return app;
}

describe("POST /api/jobs/submit – dedicated rate limiting", () => {
  const originalMax = process.env.SUBMIT_RATE_MAX;
  const originalWindow = process.env.SUBMIT_RATE_WINDOW_MS;

  beforeEach(() => {
    resetSubmitRateLimitBuckets();
    mockSendTransaction.mockReset();
    mockSendTransaction.mockResolvedValue({ id: "test-tx", status: "PENDING" });
    process.env.SUBMIT_RATE_MAX = "3";
    process.env.SUBMIT_RATE_WINDOW_MS = "60000";
  });

  afterEach(() => {
    resetSubmitRateLimitBuckets();
    if (originalMax === undefined) {
      delete process.env.SUBMIT_RATE_MAX;
    } else {
      process.env.SUBMIT_RATE_MAX = originalMax;
    }
    if (originalWindow === undefined) {
      delete process.env.SUBMIT_RATE_WINDOW_MS;
    } else {
      process.env.SUBMIT_RATE_WINDOW_MS = originalWindow;
    }
  });

  it("allows requests up to the configured threshold", async () => {
    const app = buildApp();
    for (let i = 0; i < 3; i++) {
      const res = await request(app)
        .post("/api/jobs/submit")
        .send({ signedXdr: VALID_SIGNED_XDR });
      expect(res.status).not.toBe(429);
      expect(res.headers["x-ratelimit-limit"]).toBe("3");
    }
  });

  it("returns 429 Too Many Requests once threshold is exceeded", async () => {
    const app = buildApp();
    for (let i = 0; i < 3; i++) {
      await request(app)
        .post("/api/jobs/submit")
        .send({ signedXdr: VALID_SIGNED_XDR })
        .expect(200);
    }

    const res = await request(app)
      .post("/api/jobs/submit")
      .send({ signedXdr: VALID_SIGNED_XDR })
      .expect(429);

    expect(res.body).toEqual({
      success: false,
      error: "Too many requests, please try again later",
    });
    expect(res.headers["x-ratelimit-remaining"]).toBe("0");
  });

  it("sets correct rate limit headers on each request", async () => {
    const app = buildApp();

    const first = await request(app)
      .post("/api/jobs/submit")
      .send({ signedXdr: VALID_SIGNED_XDR })
      .expect(200);
    expect(first.headers["x-ratelimit-limit"]).toBe("3");
    expect(first.headers["x-ratelimit-remaining"]).toBe("2");
    expect(first.headers["x-ratelimit-reset"]).toBeDefined();

    const second = await request(app)
      .post("/api/jobs/submit")
      .send({ signedXdr: VALID_SIGNED_XDR })
      .expect(200);
    expect(second.headers["x-ratelimit-remaining"]).toBe("1");

    const third = await request(app)
      .post("/api/jobs/submit")
      .send({ signedXdr: VALID_SIGNED_XDR })
      .expect(200);
    expect(third.headers["x-ratelimit-remaining"]).toBe("0");
  });

  it("429 response body has exactly {success, error} keys", async () => {
    const app = buildApp();
    process.env.SUBMIT_RATE_MAX = "1";

    await request(app)
      .post("/api/jobs/submit")
      .send({ signedXdr: VALID_SIGNED_XDR });

    const res = await request(app)
      .post("/api/jobs/submit")
      .send({ signedXdr: VALID_SIGNED_XDR })
      .expect(429);

    expect(Object.keys(res.body)).toEqual(["success", "error"]);
    expect(res.body.success).toBe(false);
    expect(typeof res.body.error).toBe("string");
    expect(res.body.error.length).toBeGreaterThan(0);
  });

  it("does not rate limit other job routes when submit is limited", async () => {
    const app = buildApp();
    process.env.SUBMIT_RATE_MAX = "1";

    await request(app)
      .post("/api/jobs/submit")
      .send({ signedXdr: VALID_SIGNED_XDR });
    const blocked = await request(app)
      .post("/api/jobs/submit")
      .send({ signedXdr: VALID_SIGNED_XDR });
    expect(blocked.status).toBe(429);

    const byWallet = await request(app).get("/api/jobs/by-wallet/GTESTWALLET");
    expect(byWallet.status).not.toBe(429);

    const draftRes = await request(app)
      .post("/api/jobs/create-job-draft")
      .send({
        client: "GCLIENT123",
        freelancer: "GFREELANCER123",
        arbiter: "GARBITER123",
        token: "GASTRO123",
        autoReleaseDays: 7,
        milestones: [{ amount: "100" }],
      });
    expect(draftRes.status).not.toBe(429);
  });

  it("resetSubmitRateLimitBuckets clears state between test runs", async () => {
    const app = buildApp();
    process.env.SUBMIT_RATE_MAX = "1";

    await request(app)
      .post("/api/jobs/submit")
      .send({ signedXdr: VALID_SIGNED_XDR });

    resetSubmitRateLimitBuckets();

    const afterReset = await request(app)
      .post("/api/jobs/submit")
      .send({ signedXdr: VALID_SIGNED_XDR });
    expect(afterReset.status).toBe(200);
  });

  it("uses default values when env vars are not set", async () => {
    const app = buildApp();
    delete process.env.SUBMIT_RATE_MAX;
    delete process.env.SUBMIT_RATE_WINDOW_MS;
    resetSubmitRateLimitBuckets();

    const res = await request(app)
      .post("/api/jobs/submit")
      .send({ signedXdr: VALID_SIGNED_XDR })
      .expect(200);

    expect(res.headers["x-ratelimit-limit"]).toBe("5");
  });

  it("validation errors still count against rate limit", async () => {
    const app = buildApp();
    process.env.SUBMIT_RATE_MAX = "2";

    await request(app)
      .post("/api/jobs/submit")
      .send({})
      .expect(400);

    await request(app)
      .post("/api/jobs/submit")
      .send({ signedXdr: "" })
      .expect(400);

    const third = await request(app)
      .post("/api/jobs/submit")
      .send({ signedXdr: VALID_SIGNED_XDR });
    expect(third.status).toBe(429);
  });
});
