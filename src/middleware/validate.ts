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
      const first = (result.error as ZodError).errors[0];
      sendError(res, 400, first.message);
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
export function validateWithFields(
  schema: ZodSchema,
  target: Target = "params",
  onReject?: (req: Request) => void,
) {
  return (req: Request, res: Response, next: NextFunction): void => {
    const result = schema.safeParse(req[target]);
    if (!result.success) {
      onReject?.(req);
      const errors = (result.error as ZodError).errors;
      const first = errors[0];
      
      const fields = errors.reduce((acc, err) => {
        const path = err.path.join(".");
        if (path) {
          acc[path] = err.message;
        } else {
          acc["_root"] = err.message;
        }
        return acc;
      }, {} as Record<string, string>);

      res.status(400).json({ success: false, error: first.message, fields });
      return;
    }
    
    if (target === "query") {
      (req as RequestWithValidatedQuery).validatedQuery = result.data;
    } else {
      req[target] = result.data;
    }
    next();
  };
}
