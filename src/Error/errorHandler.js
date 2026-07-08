import { ZodError } from "zod";
import logger from "../config/logger/index.js";
import { AppError } from "../Error/AppError.js";

// Maps known Postgres error codes to (statusCode, message)
function mapPgError(err) {
  switch (err.code) {
    case "23505": // unique_violation
      return { statusCode: 409, message: "Resource already exists" };
    case "23503": // foreign_key_violation
      return { statusCode: 400, message: "Related resource does not exist" };
    case "23502": // not_null_violation
      return { statusCode: 400, message: `Missing required field: ${err.column ?? "unknown"}` };
    case "22P02": // invalid_text_representation (e.g. bad enum/uuid)
      return { statusCode: 400, message: "Invalid input value" };
    default:
      return null;
  }
}

// 404 handler — mount AFTER all routes, BEFORE the error handler.
export function notFoundHandler(req, res, next) {
  next(new AppError(`Route not found: ${req.method} ${req.originalUrl}`, 404));
}

// Central error handler — mount LAST, after notFoundHandler.
// eslint-disable-next-line no-unused-vars
export function errorHandler(err, req, res, next) {
  // Known, expected/operational errors
  if (err instanceof AppError) {
    return res.status(err.statusCode).json({
      success: false,
      message: err.message,
      ...(err.details ? { details: err.details } : {}),
    });
  }

  // Zod validation errors thrown directly (e.g. schema.parse(), not safeParse())
  if (err instanceof ZodError) {
    return res.status(400).json({
      success: false,
      errors: err.flatten(),
    });
  }

  // Postgres errors (pg attaches a `code` field)
  if (err.code && typeof err.code === "string") {
    const mapped = mapPgError(err);
    if (mapped) {
      return res.status(mapped.statusCode).json({
        success: false,
        error: mapped.message,
      });
    }
  }

  // Body parser JSON errors (express.json())
  if (err.type === "entity.parse.failed") {
    return res.status(400).json({
      success: false,
      error: "Malformed JSON in request body",
    });
  }

  // Fallback: unexpected/programmer error — log full detail, hide from client
  logger.error({ err }, "Unhandled error");
  return res.status(500).json({
    success: false,
    error: "Internal server error",
  });
}