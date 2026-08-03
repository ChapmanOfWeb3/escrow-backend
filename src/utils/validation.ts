import { ZodError } from "zod";

/**
 * Convert a ZodError into a single human-readable string for API responses.
 * Keep this minimal and predictable for tests and clients.
 */
export function formatValidationError(err: ZodError): string {
  return err.errors
    .map((e) => {
      const path = e.path.length ? `${e.path.join(".")}: ` : "";
      return `${path}${e.message}`;
    })
    .join("; ");
}
