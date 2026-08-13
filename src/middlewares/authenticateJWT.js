import { verifyAccessToken } from "../utils/jwt.js";
import { AppError } from "../Error/appError.js";

/**
 * Verifies the dashboard access token (JWT) and attaches
 * req.auth = { userId, accountId, role }.
 *
 * This is intentionally separate from the (phase 2) API-key middleware:
 * /jobs/* will authenticate via API key only, dashboard/account-management
 * routes authenticate via JWT only - no route accepts either.
 */
export function authenticateJWT(req, res, next) {
  const header = req.headers.authorization;

  if (!header || !header.startsWith("Bearer ")) {
    return next(new AppError("Missing or malformed Authorization header", 401));
  }

  const token = header.slice("Bearer ".length).trim();

  try {
    req.auth = verifyAccessToken(token);
    return next();
  } catch (err) {
    if (err.name === "TokenExpiredError") {
      return next(new AppError("Access token expired", 401));
    }
    return next(new AppError("Invalid access token", 401));
  }
}
