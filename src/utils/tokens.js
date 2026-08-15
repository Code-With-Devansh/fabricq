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

/**
 * Invitation tokens are opaque like refresh tokens - only the hash is
 * stored (team_invitations.token_hash), the raw value is handed back
 * once at creation time and embedded in the accept link.
 */
export function generateInviteToken() {
  const raw = crypto.randomBytes(32).toString("base64url");
  const hash = hashToken(raw);
  return { raw, hash };
}
