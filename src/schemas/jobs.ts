import { z } from "zod";
import { StrKey } from "@stellar/stellar-sdk";
import { isValidStellarContractId, isValidStellarAddress } from "../utils/stellar.js";

// ---------------------------------------------------------------------------
// Reusable field schemas
// ---------------------------------------------------------------------------

/**
 * Validates a Soroban contract address: starts with 'C', 56 characters total,
 * and passes the Stellar SDK StrKey check.
 */
export const contractIdSchema = z
  .string({ required_error: "contractId is required" })
  .refine(isValidStellarContractId, {
    message: "contractId must be a valid Stellar contract address (C...)",
  });

/**
 * Validates a Stellar account (G…) address: starts with 'G', 56 characters,
 * passes StrKey.isValidEd25519PublicKey.
 */
export const stellarAddressSchema = z
  .string({ required_error: "address is required" })
  .refine((v) => StrKey.isValidEd25519PublicKey(v), {
    message: "address must be a valid Stellar account address (G…, 56 chars)",
  });

/**
 * Milestone index: non-negative integer (supplied as a URL param string or number).
 * Validated against the raw value before transforming, since parseInt() would
 * otherwise silently truncate decimal strings like "1.5" down to 1.
 */
export const milestoneIndexSchema = z
  .union([z.string(), z.number()])
  .refine(
    (v) => (typeof v === "number" ? Number.isInteger(v) && v >= 0 : /^\d+$/.test(v)),
    { message: "index must be a non-negative integer" },
  )
  .transform((v) => (typeof v === "number" ? v : parseInt(v, 10)));

/**
 * Amount: a positive numeric string or integer that can be coerced to BigInt.
 * Accepts strings like "100", "100000000", or plain numbers.
 */
export const amountSchema = z
  .union([z.string(), z.number(), z.bigint()])
  .refine(
    (v) => {
      try {
        const n = BigInt(v as string | number | bigint);
        return n > 0n;
      } catch {
        return false;
      }
    },
    { message: "amount must be a positive numeric value" },
  );

// ---------------------------------------------------------------------------
// Composed route schemas
// ---------------------------------------------------------------------------

/** Route params: /:contractId */
export const contractIdParamsSchema = z.object({
  contractId: contractIdSchema,
});

/** Route params: /:contractId/milestones/:index */
export const contractMilestoneParamsSchema = z.object({
  contractId: contractIdSchema,
  index: milestoneIndexSchema,
});

/** POST /build-tx body */
export const buildTxBodySchema = z.object({
  contractId: contractIdSchema,
  method: z.string({ required_error: "method is required" }).min(1, "method cannot be empty"),
  args: z.array(z.any()).optional().default([]),
  sourceAddress: stellarAddressSchema,
});

/** POST /submit body */
export const submitBodySchema = z.object({
  signedXdr: z
    .string({ required_error: "signedXdr is required" })
    .min(1, "signedXdr cannot be empty"),
});

/**
 * POST /:contractId/milestones/:index/partial-release body.
 * Keeps its pre-existing field-specific error wording (rather than the
 * generic amountSchema/stellarAddressSchema messages) since __tests__/
 * partial-release.test.ts asserts on these exact strings.
 */
export const partialReleaseBodySchema = z.object({
  amount: z
    .union([z.string(), z.number()])
    .refine(
      (val) => {
        try {
          return BigInt(String(val)) > 0n;
        } catch {
          return false;
        }
      },
      { message: "amount must be a positive integer" },
    ),
  sourceAddress: z
    .string({ required_error: "sourceAddress is required" })
    .refine(isValidStellarAddress, {
      message: "sourceAddress must be a valid Stellar account address (G...)",
    }),
});

/** POST /:contractId/milestones/:index/claim-auto-release body */
export const claimAutoReleaseBodySchema = z.object({
  sourceAddress: stellarAddressSchema,
});

/**
 * POST /create-job-draft body.
 *
 * Fields:
 *  - clientAddress   – Stellar account (G...) acting as client
 *  - freelancerAddress – Stellar account (G...) acting as freelancer
 *  - arbiterAddress  – Stellar account (G...) acting as arbiter
 *  - tokenAddress    – Stellar contract (C...) of the payment token
 *  - milestones      – non-empty array of milestone amounts (positive numeric)
 *  - title           – optional human-readable job title (max 200 chars)
 *  - description     – optional job description (max 2000 chars)
 */
export const createJobDraftBodySchema = z.object({
  clientAddress: z
    .string({ required_error: "clientAddress is required" })
    .refine(isValidStellarAddress, {
      message: "clientAddress must be a valid Stellar account address (G...)",
    }),
  freelancerAddress: z
    .string({ required_error: "freelancerAddress is required" })
    .refine(isValidStellarAddress, {
      message: "freelancerAddress must be a valid Stellar account address (G...)",
    }),
  arbiterAddress: z
    .string({ required_error: "arbiterAddress is required" })
    .refine(isValidStellarAddress, {
      message: "arbiterAddress must be a valid Stellar account address (G...)",
    }),
  tokenAddress: z
    .string({ required_error: "tokenAddress is required" })
    .refine(isValidStellarContractId, {
      message: "tokenAddress must be a valid Stellar contract address (C...)",
    }),
  milestones: z
    .array(amountSchema, { required_error: "milestones is required" })
    .min(1, "milestones must contain at least one entry"),
  title: z
    .string()
    .max(200, "title must be at most 200 characters")
    .optional(),
  description: z
    .string()
    .max(2000, "description must be at most 2000 characters")
    .optional(),
});

export type ContractIdParams = z.infer<typeof contractIdParamsSchema>;
export type ContractMilestoneParams = z.infer<typeof contractMilestoneParamsSchema>;
export type BuildTxBody = z.infer<typeof buildTxBodySchema>;
export type SubmitBody = z.infer<typeof submitBodySchema>;
export type PartialReleaseBody = z.infer<typeof partialReleaseBodySchema>;
export type ClaimAutoReleaseBody = z.infer<typeof claimAutoReleaseBodySchema>;
export type CreateJobDraftBody = z.infer<typeof createJobDraftBodySchema>;
