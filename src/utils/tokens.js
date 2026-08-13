import crypto from "crypto";

/**
 * Refresh tokens are opaque (not JWTs) so they can be revoked/rotated
 * server-side. The raw token is returned to the client once; only its
 * hash is persisted, mirroring how the api_keys design will store
 * key_hash instead of the raw fq_live_ secret.
 */
export function generateRefreshToken() {
  const raw = crypto.randomBytes(32).toString("base64url"); // 256 bits
  const hash = hashToken(raw);
  return { raw, hash };
}

export function hashToken(raw) {
  return crypto.createHash("sha256").update(raw).digest("hex");
}
