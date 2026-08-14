import crypto from "crypto";

const KEY_PREFIX = "fq_live_";
// Length of key_prefix stored/shown for identification - the literal
// prefix plus a few chars of the secret, enough to tell keys apart in a
// UI list without revealing anything usable.
const DISPLAY_PREFIX_LENGTH = KEY_PREFIX.length + 6;

/**
 * Generates a new API key. Returns the raw key (shown to the client
 * exactly once), the short display prefix (safe to store/show forever),
 * and the sha256 hash (what's actually persisted for verification).
 */
export function generateApiKey() {
  const secret = crypto.randomBytes(24).toString("base64url"); // 192 bits
  const raw = `${KEY_PREFIX}${secret}`;
  return {
    raw,
    prefix: raw.slice(0, DISPLAY_PREFIX_LENGTH),
    hash: hashApiKey(raw),
  };
}

export function hashApiKey(raw) {
  return crypto.createHash("sha256").update(raw).digest("hex");
}

export function extractPrefix(raw) {
  return raw.slice(0, DISPLAY_PREFIX_LENGTH);
}

export function looksLikeApiKey(raw) {
  return typeof raw === "string" && raw.startsWith(KEY_PREFIX);
}
