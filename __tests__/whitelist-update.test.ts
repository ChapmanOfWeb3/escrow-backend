import { jest } from "@jest/globals";
import request from "supertest";
import express from "express";
import {
  resetJobWhitelistRateLimitBuckets,
  resetWhitelistUpdateRateLimitBuckets,
} from "../src/middleware/job-contract-rate-limit.js";

const VALID_CONTRACT =
  "CDD5WKK3WT3QVKXMXTJNDIXE4T73FK6GGXDSD6UTJAH6YYZU52SQ4MUH";

// Valid Stellar account addresses (G...) and contract address (C...)
const VALID_STELLAR_ADDRESS_1 =
  "GAODBHVR63Z56MVQRBEJSYM2H5423LJ4WAPUUBOFG4JYY72S6ROKVZRX";
const VALID_STELLAR_ADDRESS_2 =
  "GABNCQRZNTG6MMITD33VHFITKJZ5PSYW2XVEXMP52BSMTPLU7WORDQNT";

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

const { default: router, resetWhitelistCache } = await import(
  "../src/routes/jobs.js"
);

function buildApp() {
  const app = express();
  app.use(express.json());
  app.use("/api/jobs", router);
  return app;
}

describe("POST /api/jobs/:contractId/whitelist/update", () => {
  beforeEach(() => {
    mockGetAccount.mockReset();
    mockSimulateTransaction.mockReset();
    mockLoggerInfo.mockReset();
    mockLoggerWarn.mockReset();
    mockLoggerError.mockReset();
    resetJobWhitelistRateLimitBuckets();
    resetWhitelistUpdateRateLimitBuckets();
    resetWhitelistCache();

    delete process.env.API_KEY;
    delete process.env.JOB_WHITELIST_RATE_MAX;
    delete process.env.JOB_WHITELIST_RATE_WINDOW_MS;
    delete process.env.JOB_WHITELIST_UPDATE_RATE_MAX;
    delete process.env.JOB_WHITELIST_UPDATE_RATE_WINDOW_MS;
    delete process.env.ALLOWED_ORIGINS;

    mockGetAccount.mockResolvedValue({
      accountId: () =>
        "GAODBHVR63Z56MVQRBEJSYM2H5423LJ4WAPUUBOFG4JYY72S6ROKVZRX",
      sequenceNumber: () => "123456789",
      incrementSequenceNumber: () => {},
    });

    mockSimulateTransaction.mockResolvedValue({
      result: {
        retval: {
          forEach: (fn: (item: unknown) => void) => {
            fn({ toString: () => "TOKEN1" });
          },
        },
      },
    });
  });

  it("Test 1 — Valid Stellar address accepted", async () => {
    const res = await request(buildApp())
      .post(`/api/jobs/${VALID_CONTRACT}/whitelist/update`)
      .send({
        addresses: [VALID_STELLAR_ADDRESS_1],
      });

    expect(res.status).toBe(200);
    expect(res.body.success).toBe(true);
    expect(res.body.data.addresses).toEqual([VALID_STELLAR_ADDRESS_1]);
    expect(res.body.data.contractId).toBe(VALID_CONTRACT);
    expect(res.body.data.updated).toBe(true);
  });

  it("Test 2 — Clearly invalid address returns 400 Bad Request", async () => {
    const res = await request(buildApp())
      .post(`/api/jobs/${VALID_CONTRACT}/whitelist/update`)
      .send({
        addresses: ["invalid-address"],
      });

    expect(res.status).toBe(400);
    expect(res.body.success).toBe(false);
    expect(res.body.error).toBe("ValidationError");
    expect(res.body.details).toBeDefined();
    expect(res.body.details[0].message).toMatch(/Invalid Stellar address/i);
  });

  it("Test 3 — Malformed Stellar-looking address returns 400 Bad Request", async () => {
    const malformedAddress = "G" + "A".repeat(55);

    const res = await request(buildApp())
      .post(`/api/jobs/${VALID_CONTRACT}/whitelist/update`)
      .send({
        addresses: [malformedAddress],
      });

    expect(res.status).toBe(400);
    expect(res.body.success).toBe(false);
    expect(res.body.error).toBe("ValidationError");
    expect(res.body.details[0].message).toMatch(/Invalid Stellar address/i);
  });

  it("Test 4 — Multiple addresses with one invalid address rejects the entire request with 400 Bad Request", async () => {
    const res = await request(buildApp())
      .post(`/api/jobs/${VALID_CONTRACT}/whitelist/update`)
      .send({
        addresses: [
          VALID_STELLAR_ADDRESS_1,
          "invalid-address",
          VALID_STELLAR_ADDRESS_2,
        ],
      });

    expect(res.status).toBe(400);
    expect(res.body.success).toBe(false);
    expect(res.body.error).toBe("ValidationError");
    expect(res.body.details[0].message).toMatch(/Invalid Stellar address/i);
  });

  it("Test 5 — Multiple valid addresses accepted", async () => {
    const res = await request(buildApp())
      .post(`/api/jobs/${VALID_CONTRACT}/whitelist/update`)
      .send({
        addresses: [
          VALID_STELLAR_ADDRESS_1,
          VALID_STELLAR_ADDRESS_2,
          VALID_CONTRACT,
        ],
      });

    expect(res.status).toBe(200);
    expect(res.body.success).toBe(true);
    expect(res.body.data.addresses).toHaveLength(3);
  });

  it("Test 6 — Missing or empty address input returns 400 Bad Request", async () => {
    const resMissing = await request(buildApp())
      .post(`/api/jobs/${VALID_CONTRACT}/whitelist/update`)
      .send({});

    expect(resMissing.status).toBe(400);
    expect(resMissing.body.success).toBe(false);

    const resEmpty = await request(buildApp())
      .post(`/api/jobs/${VALID_CONTRACT}/whitelist/update`)
      .send({ addresses: [] });

    expect(resEmpty.status).toBe(400);
    expect(resEmpty.body.success).toBe(false);
  });

  it("returns 400 if contractId in URL path is invalid", async () => {
    const res = await request(buildApp())
      .post("/api/jobs/invalid-contract-id/whitelist/update")
      .send({
        addresses: [VALID_STELLAR_ADDRESS_1],
      });

    expect(res.status).toBe(400);
    expect(res.body.success).toBe(false);
    expect(res.body.error).toBe("ValidationError");
  });

  // ─────────────────────────────────────────────────────────────────────────
  // Issue #241 additions: 404, 401, 500 & standard response format checks
  // ─────────────────────────────────────────────────────────────────────────

  describe("Contract not found: returns 404", () => {
    it("returns 404 when simulation reports contract not found", async () => {
      mockSimulateTransaction.mockResolvedValue({
        error: "contract not found on network",
      });

      const res = await request(buildApp())
        .post(`/api/jobs/${VALID_CONTRACT}/whitelist/update`)
        .send({ addresses: [VALID_STELLAR_ADDRESS_1] })
        .expect(404);

      expect(res.body).toEqual({
        success: false,
        error: "Job not found",
      });
    });

    it("returns 404 when simulation reports contract error #1", async () => {
      mockSimulateTransaction.mockResolvedValue({
        error: "contract error #1",
      });

      const res = await request(buildApp())
        .post(`/api/jobs/${VALID_CONTRACT}/whitelist/update`)
        .send({ addresses: [VALID_STELLAR_ADDRESS_1] })
        .expect(404);

      expect(res.body).toEqual({
        success: false,
        error: "Job not found",
      });
    });
  });

  describe("Authorization and 401 handling", () => {
    it("returns 401 when API key is required but missing", async () => {
      process.env.API_KEY = "secret-key";

      const res = await request(buildApp())
        .post(`/api/jobs/${VALID_CONTRACT}/whitelist/update`)
        .send({ addresses: [VALID_STELLAR_ADDRESS_1] })
        .expect(401);

      expect(res.body).toEqual({
        success: false,
        error: "Unauthorized",
      });
    });

    it("returns 401 when provided API key is invalid", async () => {
      process.env.API_KEY = "secret-key";

      const res = await request(buildApp())
        .post(`/api/jobs/${VALID_CONTRACT}/whitelist/update`)
        .set("x-api-key", "wrong-key")
        .send({ addresses: [VALID_STELLAR_ADDRESS_1] })
        .expect(401);

      expect(res.body).toEqual({
        success: false,
        error: "Unauthorized",
      });
    });

    it("accepts request when valid API key is provided", async () => {
      process.env.API_KEY = "secret-key";

      const res = await request(buildApp())
        .post(`/api/jobs/${VALID_CONTRACT}/whitelist/update`)
        .set("x-api-key", "secret-key")
        .send({ addresses: [VALID_STELLAR_ADDRESS_1] })
        .expect(200);

      expect(res.body.success).toBe(true);
    });
  });

  describe("Unexpected internal server error: returns 500", () => {
    it("returns 500 when RPC simulation throws an unexpected error", async () => {
      mockSimulateTransaction.mockRejectedValue(
        new Error("RPC connection refused - server at 10.0.0.5:8000")
      );

      const res = await request(buildApp())
        .post(`/api/jobs/${VALID_CONTRACT}/whitelist/update`)
        .send({ addresses: [VALID_STELLAR_ADDRESS_1] })
        .expect(500);

      expect(res.body).toEqual({
        success: false,
        error: "Internal server error",
      });
      expect(JSON.stringify(res.body)).not.toContain("10.0.0.5");
      expect(JSON.stringify(res.body)).not.toContain("connection refused");
    });

    it("logs error server-side on 500 failure", async () => {
      mockSimulateTransaction.mockRejectedValue(new Error("RPC internal failure"));

      await request(buildApp())
        .post(`/api/jobs/${VALID_CONTRACT}/whitelist/update`)
        .send({ addresses: [VALID_STELLAR_ADDRESS_1] })
        .expect(500);

      expect(mockLoggerError).toHaveBeenCalledWith(
        "Failed to update whitelist",
        { contractId: VALID_CONTRACT, error: "RPC internal failure" }
      );
    });
  });

  // ─────────────────────────────────────────────────────────────────────────
  // Issue #242 additions: CORS and security headers
  // ─────────────────────────────────────────────────────────────────────────

  describe("CORS and Security Headers (Issue #242)", () => {
    it("allows trusted origins and sets CORS response headers", async () => {
      process.env.ALLOWED_ORIGINS = "https://app.example.com";

      const res = await request(buildApp())
        .post(`/api/jobs/${VALID_CONTRACT}/whitelist/update`)
        .set("Origin", "https://app.example.com")
        .send({ addresses: [VALID_STELLAR_ADDRESS_1] });

      expect(res.status).toBe(200);
      expect(res.headers["access-control-allow-origin"]).toBe(
        "https://app.example.com"
      );
      expect(res.headers.vary).toContain("Origin");
    });

    it("rejects requests from unauthorized origins with 403", async () => {
      process.env.ALLOWED_ORIGINS = "https://app.example.com";

      const res = await request(buildApp())
        .post(`/api/jobs/${VALID_CONTRACT}/whitelist/update`)
        .set("Origin", "https://malicious.example.com")
        .send({ addresses: [VALID_STELLAR_ADDRESS_1] })
        .expect(403);

      expect(res.body).toEqual({
        success: false,
        error: "Origin not allowed by CORS policy",
      });
      expect(res.headers["access-control-allow-origin"]).toBeUndefined();
    });

    it("handles OPTIONS preflight for authorized origin", async () => {
      process.env.ALLOWED_ORIGINS = "https://app.example.com";

      const res = await request(buildApp())
        .options(`/api/jobs/${VALID_CONTRACT}/whitelist/update`)
        .set("Origin", "https://app.example.com")
        .set("Access-Control-Request-Method", "POST")
        .expect(204);

      expect(res.headers["access-control-allow-origin"]).toBe(
        "https://app.example.com"
      );
      expect(res.headers["access-control-allow-methods"]).toBe(
        "POST, OPTIONS"
      );
    });

    it("rejects OPTIONS preflight for unauthorized origin with 403", async () => {
      process.env.ALLOWED_ORIGINS = "https://app.example.com";

      const res = await request(buildApp())
        .options(`/api/jobs/${VALID_CONTRACT}/whitelist/update`)
        .set("Origin", "https://malicious.example.com")
        .set("Access-Control-Request-Method", "POST")
        .expect(403);

      expect(res.headers["access-control-allow-origin"]).toBeUndefined();
    });

    it("allows requests without an Origin header", async () => {
      process.env.ALLOWED_ORIGINS = "https://app.example.com";

      const res = await request(buildApp())
        .post(`/api/jobs/${VALID_CONTRACT}/whitelist/update`)
        .send({ addresses: [VALID_STELLAR_ADDRESS_1] });

      expect(res.status).toBe(200);
      expect(res.headers["access-control-allow-origin"]).toBeUndefined();
    });

    it("applies security headers on POST /api/jobs/:contractId/whitelist/update", async () => {
      const res = await request(buildApp())
        .post(`/api/jobs/${VALID_CONTRACT}/whitelist/update`)
        .send({ addresses: [VALID_STELLAR_ADDRESS_1] });

      expect(res.headers["x-content-type-options"]).toBe("nosniff");
      expect(res.headers["x-frame-options"]).toBe("DENY");
      expect(res.headers["referrer-policy"]).toBe("no-referrer");
      expect(res.headers["content-security-policy"]).toBe("default-src 'none'");
    });
  });
});
