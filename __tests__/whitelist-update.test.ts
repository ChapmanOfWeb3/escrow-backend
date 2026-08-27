import request from "supertest";
import express from "express";
import router from "../src/routes/jobs.js";

const app = express();
app.use(express.json());
app.use("/api/jobs", router);

const VALID_CONTRACT = "CDD5WKK3WT3QVKXMXTJNDIXE4T73FK6GGXDSD6UTJAH6YYZU52SQ4MUH";

// Valid Stellar account addresses (G...) and contract address (C...)
const VALID_STELLAR_ADDRESS_1 = "GAODBHVR63Z56MVQRBEJSYM2H5423LJ4WAPUUBOFG4JYY72S6ROKVZRX";
const VALID_STELLAR_ADDRESS_2 = "GABNCQRZNTG6MMITD33VHFITKJZ5PSYW2XVEXMP52BSMTPLU7WORDQNT";

describe("POST /api/jobs/:contractId/whitelist/update", () => {
  it("Test 1 — Valid Stellar address accepted", async () => {
    const res = await request(app)
      .post(`/api/jobs/${VALID_CONTRACT}/whitelist/update`)
      .send({
        addresses: [VALID_STELLAR_ADDRESS_1],
      });

    expect(res.status).toBe(200);
    expect(res.body.success).toBe(true);
    expect(res.body.data.addresses).toEqual([VALID_STELLAR_ADDRESS_1]);
  });

  it("Test 2 — Clearly invalid address returns 400 Bad Request", async () => {
    const res = await request(app)
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
    // 56 characters starting with G, but invalid checksum
    const malformedAddress = "G" + "A".repeat(55);

    const res = await request(app)
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
    const res = await request(app)
      .post(`/api/jobs/${VALID_CONTRACT}/whitelist/update`)
      .send({
        addresses: [VALID_STELLAR_ADDRESS_1, "invalid-address", VALID_STELLAR_ADDRESS_2],
      });

    expect(res.status).toBe(400);
    expect(res.body.success).toBe(false);
    expect(res.body.error).toBe("ValidationError");
    expect(res.body.details[0].message).toMatch(/Invalid Stellar address/i);
  });

  it("Test 5 — Multiple valid addresses accepted", async () => {
    const res = await request(app)
      .post(`/api/jobs/${VALID_CONTRACT}/whitelist/update`)
      .send({
        addresses: [VALID_STELLAR_ADDRESS_1, VALID_STELLAR_ADDRESS_2, VALID_CONTRACT],
      });

    expect(res.status).toBe(200);
    expect(res.body.success).toBe(true);
    expect(res.body.data.addresses).toHaveLength(3);
  });

  it("Test 6 — Missing or empty address input returns 400 Bad Request", async () => {
    const resMissing = await request(app)
      .post(`/api/jobs/${VALID_CONTRACT}/whitelist/update`)
      .send({});

    expect(resMissing.status).toBe(400);
    expect(resMissing.body.success).toBe(false);

    const resEmpty = await request(app)
      .post(`/api/jobs/${VALID_CONTRACT}/whitelist/update`)
      .send({ addresses: [] });

    expect(resEmpty.status).toBe(400);
    expect(resEmpty.body.success).toBe(false);
  });

  it("returns 400 if contractId in URL path is invalid", async () => {
    const res = await request(app)
      .post("/api/jobs/invalid-contract-id/whitelist/update")
      .send({
        addresses: [VALID_STELLAR_ADDRESS_1],
      });

    expect(res.status).toBe(400);
    expect(res.body.success).toBe(false);
    expect(res.body.error).toBe("ValidationError");
  });
});
