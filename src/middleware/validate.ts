import type { NextFunction, Request, Response } from "express";
import { ZodSchema, ZodError } from "zod";
import { sendError } from "../utils/api-response.js";

type Target = "params" | "body" | "query";

function formatValidationError(error: ZodError): string {
  const issues = error.errors;
  if (issues.length === 1) {
    return issues[0].message;
  }

  return issues
    .map((issue) => {
      const path = issue.path.length > 0 ? issue.path.join(".") : "value";
      return `${path}: ${issue.message}`;
    })
    .join("; ");
}

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
    req[target] = result.data;
    next();
  };
}
