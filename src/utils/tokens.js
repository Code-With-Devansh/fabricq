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
 * Two-factor codes are checked by comparing hashes (fixed-length hex
 * strings), so a plain === would leak timing info about how many
 * leading characters matched. Used instead of the DB-side WHERE
 * token_hash = $1 lookups elsewhere, since those go through an index
 * and aren't a JS-level string compare in the first place.
 */
export function constantTimeEqual(a, b) {
  const bufA = Buffer.from(a);
  const bufB = Buffer.from(b);
  if (bufA.length !== bufB.length) return false;
  return crypto.timingSafeEqual(bufA, bufB);
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

/**
 * Same opaque-token shape as generateInviteToken, reused for email
 * verification and password reset links - both are "click a link with
 * a long random secret in it" flows, just against different tables.
 */
export function generateOpaqueToken() {
  const raw = crypto.randomBytes(32).toString("base64url");
  const hash = hashToken(raw);
  return { raw, hash };
}

/**
 * 6-digit numeric OTP for email-based 2FA. Unlike the link tokens
 * above, this is short and typed by hand, so it's a bounded numeric
 * range rather than a long base62/base64url string. Only the hash is
 * stored, same discipline as everything else here.
 */
export function generateOtpCode() {
  const code = crypto.randomInt(0, 1_000_000).toString().padStart(6, "0");
  const hash = hashToken(code);
  return { code, hash };
}
