import type { NextFunction, Request, Response } from "express";
import { ZodSchema, ZodError } from "zod";
import { sendError } from "../utils/api-response.js";

type Target = "params" | "body" | "query";

export type RequestWithValidatedQuery = Request & {
  validatedQuery?: unknown;
};

export function validate(
  schema: ZodSchema,
  target: Target = "params",
  onReject?: (req: Request) => void,
) {
  return (req: Request, res: Response, next: NextFunction): void => {
    const result = schema.safeParse(req[target]);
    if (!result.success) {
      onReject?.(req);
      sendError(res, 400, formatValidationError(result.error as ZodError));
      return;
    }
    // Express 5 exposes req.query as a getter-only property, so validated
    // query data is attached on a custom field instead of reassignment.
    if (target === "query") {
      (req as RequestWithValidatedQuery).validatedQuery = result.data;
    } else {
      req[target] = result.data;
    }
    next();
  };
}
