import { verifyAccessToken, isTokenExpiredError } from "../utils/jwt.js";
import { AppError } from "../Error/appError.js";

/**
 * Verifies the dashboard access token (JWT) and attaches
 * req.auth = { userId }.
 *
 * Identity only - no team_id/role. Any route that needs a role/permission
 * check for a specific team must also run loadTeamContext, which resolves
 * that per (team, user) via team_memberships.
 *
 * This stays separate from the (phase 2) API-key middleware: /jobs/* will
 * authenticate via API key only, dashboard/team-management routes via JWT
 * only - no route accepts either.
 */
export async function authenticateJWT(req, res, next) {
  const header = req.headers.authorization;

  if (!header || !header.startsWith("Bearer ")) {
    return next(new AppError("Missing or malformed Authorization header", 401));
  }

  const token = header.slice("Bearer ".length).trim();

  try {
    req.auth = await verifyAccessToken(token);
    return next();
  } catch (err) {
    if (isTokenExpiredError(err)) {
      return next(new AppError("Access token expired", 401));
    }
    return next(new AppError("Invalid access token", 401));
  }
}
