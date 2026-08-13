import { SignJWT, jwtVerify, errors as joseErrors } from "jose";
import config from "../config/index.js";

const { jwtSecret, accessTokenTtlSeconds, issuer, audience } = config.auth;

const secret = new TextEncoder().encode(jwtSecret);

/**
 * Signs a dashboard access token. Identity only - sub, iss, aud, exp.
 *
 * No account_id/role here on purpose: a user can belong to several teams
 * with a different role in each, so "role" isn't a property of the user
 * and can't live on a single token. Team-scoped requests resolve role/
 * permissions per-request via team_memberships (see loadTeamContext).
 */
export function signAccessToken({ userId }) {
  return new SignJWT({})
    .setProtectedHeader({ alg: "HS256", typ: "JWT" })
    .setSubject(userId)
    .setIssuer(issuer)
    .setAudience(audience)
    .setIssuedAt()
    .setExpirationTime(`${accessTokenTtlSeconds}s`)
    .sign(secret);
}

/**
 * Verifies a dashboard access token. Throws on invalid/expired tokens -
 * let the caller decide how to translate that into an HTTP response.
 */
export async function verifyAccessToken(token) {
  const { payload } = await jwtVerify(token, secret, {
    algorithms: ["HS256"],
    issuer,
    audience,
  });

  return { userId: payload.sub };
}

export function isTokenExpiredError(err) {
  return err instanceof joseErrors.JWTExpired;
}
