import { SignJWT, jwtVerify } from "jose";
import config from "../config/index.js";

const {
  jwtSecret,
  accessTokenTtlSeconds,
  issuer,
  audience,
} = config.auth;

const secret = new TextEncoder().encode(jwtSecret);

/**
 * Signs a dashboard access token.
 */
export function signAccessToken({ userId, accountId, role }) {
  console.log({userId, accountId, role})
  return new SignJWT({
    account_id: accountId,
    role,
  })
    .setProtectedHeader({
      alg: "HS256",
      typ: "JWT",
    })
    .setSubject(userId)
    .setIssuer(issuer)
    .setAudience(audience)
    .setIssuedAt()
    .setExpirationTime(`${accessTokenTtlSeconds}s`)
    .sign(secret);
}

/**
 * Verifies a dashboard access token.
 */
export async function verifyAccessToken(token) {
  const { payload } = await jwtVerify(token, secret, {
    algorithms: ["HS256"],
    issuer,
    audience,
  });

  return {
    userId: payload.sub,
    accountId: payload.account_id,
    role: payload.role,
  };
}