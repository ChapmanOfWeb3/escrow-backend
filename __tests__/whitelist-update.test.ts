import { jest } from "@jest/globals";
import request from "supertest";
import express from "express";

const VALID_CONTRACT = "CDD5WKK3WT3QVKXMXTJNDIXE4T73FK6GGXDSD6UTJAH6YYZU52SQ4MUH";
const VALID_ADDRESS = "GAODBHVR63Z56MVQRBEJSYM2H5423LJ4WAPUUBOFG4JYY72S6ROKVZRX";
const VALID_ADDRESS_2 = "GB2AAQ5ECB3LG5XN7VJQ5T7VBR2DXBVXA5HH24376WIFPE7PQN6HBT5X";

const mockGetAccount = jest.fn<() => Promise<unknown>>();
const mockPrepareTransaction = jest.fn<() => Promise<unknown>>();

jest.unstable_mockModule("@stellar/stellar-sdk/rpc", () => ({
  Server: class MockServer {
    getAccount = mockGetAccount;
    prepareTransaction = mockPrepareTransaction;
  },
}));

jest.unstable_mockModule("../src/utils/logger.js", () => ({
  default: { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn() },
}));

const { default: router, resetWhitelistUpdateCache } = await import("../src/routes/jobs.js");
const { resetJobWhitelistRateLimitBuckets } = await import(
  "../src/middleware/job-contract-rate-limit.js"
);

function buildApp() {
  const app = express();
  app.use(express.json());
  app.use("/api/jobs", router);
  return app;
}

const ENDPOINT = `/api/jobs/${VALID_CONTRACT}/whitelist/update`;
const VALID_BODY = {
  token: VALID_CONTRACT,
  action: "add" as const,
  sourceAddress: VALID_ADDRESS,
};

const MOCK_ACCOUNT = {
  accountId: () => VALID_ADDRESS,
  sequenceNumber: () => "1",
  incrementSequenceNumber: () => {},
};

describe("POST /api/jobs/:contractId/whitelist/update (#246, #248)", () => {
  const originalApiKey = process.env.API_KEY;
  const originalAllowedOrigins = process.env.ALLOWED_ORIGINS;

  beforeEach(() => {
    delete process.env.API_KEY;
    delete process.env.ALLOWED_ORIGINS;
    resetWhitelistUpdateCache();
    resetJobWhitelistRateLimitBuckets();
    mockGetAccount.mockReset();
    mockPrepareTransaction.mockReset();
    mockGetAccount.mockResolvedValue(MOCK_ACCOUNT);
    mockPrepareTransaction.mockResolvedValue({ toXDR: () => "AAAAAQ==" });
  });

  afterEach(() => {
    if (originalApiKey === undefined) delete process.env.API_KEY;
    else process.env.API_KEY = originalApiKey;
    if (originalAllowedOrigins === undefined) delete process.env.ALLOWED_ORIGINS;
    else process.env.ALLOWED_ORIGINS = originalAllowedOrigins;
  });

  // -----------------------------------------------------------------------
  // Params validation
  // -----------------------------------------------------------------------

  describe("params validation", () => {
    it("returns 400 for an invalid contractId", async () => {
      const res = await request(buildApp())
        .post("/api/jobs/not-a-valid-contract/whitelist/update")
        .send(VALID_BODY)
        .expect(400);

      expect(res.body).toMatchObject({ success: false, error: expect.any(String) });
      expect(mockGetAccount).not.toHaveBeenCalled();
    });

    it("returns 400 when contractId is a G... account address", async () => {
      const res = await request(buildApp())
        .post(`/api/jobs/${VALID_ADDRESS}/whitelist/update`)
        .send(VALID_BODY)
        .expect(400);

      expect(res.body.success).toBe(false);
      expect(res.body.error).toBe("ValidationError");
    });
  });

  // -----------------------------------------------------------------------
  // Body validation
  // -----------------------------------------------------------------------

  describe("body validation", () => {
    it("returns 400 when token is missing", async () => {
      const res = await request(buildApp())
        .post(ENDPOINT)
        .send({ action: "add", sourceAddress: VALID_ADDRESS })
        .expect(400);

      expect(res.body.error).toBe("ValidationError");
      expect(mockGetAccount).not.toHaveBeenCalled();
    });

    it("returns 400 when token is not a valid contract address", async () => {
      const res = await request(buildApp())
        .post(ENDPOINT)
        .send({ ...VALID_BODY, token: "not-a-token" })
        .expect(400);

      expect(res.body.error).toBe("ValidationError");
    });

    it("returns 400 when action is missing", async () => {
      const res = await request(buildApp())
        .post(ENDPOINT)
        .send({ token: VALID_CONTRACT, sourceAddress: VALID_ADDRESS })
        .expect(400);

      expect(res.body.error).toBe("ValidationError");
    });

    it("returns 400 when action is not add or remove", async () => {
      const res = await request(buildApp())
        .post(ENDPOINT)
        .send({ ...VALID_BODY, action: "toggle" })
        .expect(400);

      expect(res.body.error).toBe("ValidationError");
    });

    it("returns 400 when sourceAddress is missing", async () => {
      const res = await request(buildApp())
        .post(ENDPOINT)
        .send({ token: VALID_CONTRACT, action: "add" })
        .expect(400);

      expect(res.body.error).toBe("ValidationError");
    });

    it("returns 400 when sourceAddress is invalid", async () => {
      const res = await request(buildApp())
        .post(ENDPOINT)
        .send({ ...VALID_BODY, sourceAddress: "not-valid" })
        .expect(400);

      expect(res.body.error).toBe("ValidationError");
    });

    it("returns 400 when body contains extra fields", async () => {
      const res = await request(buildApp())
        .post(ENDPOINT)
        .send({ ...VALID_BODY, extra: true })
        .expect(400);

      expect(res.body.success).toBe(false);
      expect(res.body.details[0].message).toMatch(/unrecognized key/i);
    });
  });

  // -----------------------------------------------------------------------
  // Auth
  // -----------------------------------------------------------------------

  describe("API key auth", () => {
    it("returns 401 when API_KEY is set and header is missing", async () => {
      process.env.API_KEY = "secret-key";
      const res = await request(buildApp()).post(ENDPOINT).send(VALID_BODY).expect(401);
      expect(res.body).toMatchObject({ success: false, error: "Unauthorized" });
      expect(mockGetAccount).not.toHaveBeenCalled();
    });

    it("returns 401 when API_KEY is set and header is wrong", async () => {
      process.env.API_KEY = "secret-key";
      const res = await request(buildApp())
        .post(ENDPOINT)
        .set("x-api-key", "wrong")
        .send(VALID_BODY)
        .expect(401);
      expect(res.body.error).toBe("Unauthorized");
    });

    it("succeeds when API_KEY matches", async () => {
      process.env.API_KEY = "secret-key";
      const res = await request(buildApp())
        .post(ENDPOINT)
        .set("x-api-key", "secret-key")
        .send(VALID_BODY)
        .expect(200);
      expect(res.body).toEqual({ success: true, xdr: "AAAAAQ==" });
    });
  });

  // -----------------------------------------------------------------------
  // Success paths
  // -----------------------------------------------------------------------

  describe("success paths", () => {
    it("returns XDR for action=add", async () => {
      const res = await request(buildApp()).post(ENDPOINT).send(VALID_BODY).expect(200);
      expect(res.body).toEqual({ success: true, xdr: "AAAAAQ==" });
      expect(mockGetAccount).toHaveBeenCalledWith(VALID_ADDRESS);
      expect(mockPrepareTransaction).toHaveBeenCalledTimes(1);
    });

    it("returns XDR for action=remove", async () => {
      const res = await request(buildApp())
        .post(ENDPOINT)
        .send({ ...VALID_BODY, action: "remove" })
        .expect(200);
      expect(res.body).toEqual({ success: true, xdr: "AAAAAQ==" });
      expect(mockPrepareTransaction).toHaveBeenCalledTimes(1);
    });
  });

  // -----------------------------------------------------------------------
  // Node-Cache (#246)
  // -----------------------------------------------------------------------

  describe("Node-Cache lookup values (#246)", () => {
    it("serves a sequential second request from in-memory cache", async () => {
      const app = buildApp();
      await request(app).post(ENDPOINT).send(VALID_BODY).expect(200);
      await request(app).post(ENDPOINT).send(VALID_BODY).expect(200);
      expect(mockPrepareTransaction).toHaveBeenCalledTimes(1);
      expect(mockGetAccount).toHaveBeenCalledTimes(1);
    });

    it("concurrent identical requests share one in-flight RPC and pull from cache", async () => {
      mockPrepareTransaction.mockImplementation(
        () =>
          new Promise((resolve) =>
            setTimeout(() => resolve({ toXDR: () => "AAAAAQ==" }), 40),
          ),
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
      expect(mockPrepareTransaction).toHaveBeenCalledTimes(1);
      expect(mockGetAccount).toHaveBeenCalledTimes(1);
    });

    it("uses distinct cache keys for different actions", async () => {
      const app = buildApp();
      await request(app).post(ENDPOINT).send(VALID_BODY).expect(200);
      await request(app)
        .post(ENDPOINT)
        .send({ ...VALID_BODY, action: "remove" })
        .expect(200);
      expect(mockPrepareTransaction).toHaveBeenCalledTimes(2);
    });

    it("uses distinct cache keys for different sourceAddress values", async () => {
      const app = buildApp();
      await request(app).post(ENDPOINT).send(VALID_BODY).expect(200);
      await request(app)
        .post(ENDPOINT)
        .send({ ...VALID_BODY, sourceAddress: VALID_ADDRESS_2 })
        .expect(200);
      expect(mockPrepareTransaction).toHaveBeenCalledTimes(2);
    });

    it("does not reuse a failed RPC result from cache", async () => {
      mockPrepareTransaction
        .mockRejectedValueOnce(new Error("rpc down"))
        .mockResolvedValueOnce({ toXDR: () => "AAAAAQ==" });

      const app = buildApp();
      const fail = await request(app).post(ENDPOINT).send(VALID_BODY).expect(500);
      expect(fail.body).toEqual({ success: false, error: "Internal server error" });
      expect(fail.body).not.toHaveProperty("stack");

      const ok = await request(app).post(ENDPOINT).send(VALID_BODY).expect(200);
      expect(ok.body.xdr).toBe("AAAAAQ==");
      expect(mockPrepareTransaction).toHaveBeenCalledTimes(2);
    });
  });

  // -----------------------------------------------------------------------
  // Error paths
  // -----------------------------------------------------------------------

  describe("error paths", () => {
    it("returns sanitized 500 when getAccount fails", async () => {
      mockGetAccount.mockRejectedValue(new Error("account not found"));
      const res = await request(buildApp()).post(ENDPOINT).send(VALID_BODY).expect(500);
      expect(res.body).toEqual({ success: false, error: "Internal server error" });
      expect(JSON.stringify(res.body)).not.toMatch(/account not found|stack/i);
    });

    it("returns sanitized 500 when prepareTransaction fails", async () => {
      mockPrepareTransaction.mockRejectedValue(new Error("simulation failed"));
      const res = await request(buildApp()).post(ENDPOINT).send(VALID_BODY).expect(500);
      expect(res.body).toEqual({ success: false, error: "Internal server error" });
    });
  });
});
