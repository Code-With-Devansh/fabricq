import { AppError } from "../Error/appError.js";
import { hashApiKey, extractPrefix, looksLikeApiKey } from "../utils/apiKey.js";
import { findByPrefix, touchLastUsed } from "../repositories/apiKey.repository.js";

/**
 * Authenticates the public API (/v1/*) via `Authorization: Bearer fq_live_...`.
 * Attaches req.apiKey = { keyId, teamId, scopes: Set }.
 *
 * Separate from authenticateJWT by design: /v1/* never accepts a dashboard
 * JWT, and /teams/*  never accepts an API key - two authentication systems,
 * no route accepts either.
 */
export async function authenticateApiKey(req, res, next) {
  const header = req.headers.authorization;
  if (!header || !header.startsWith("Bearer ")) {
    return next(new AppError("Missing or malformed Authorization header", 401));
  }

  const raw = header.slice("Bearer ".length).trim();
  if (!looksLikeApiKey(raw)) {
    return next(new AppError("Invalid API key", 401));
  }

  try {
    const candidates = await findByPrefix(extractPrefix(raw));
    const hash = hashApiKey(raw);
    const match = candidates.find((k) => k.key_hash === hash);

    if (!match) {
      return next(new AppError("Invalid API key", 401));
    }
    if (match.revoked_at) {
      return next(new AppError("This API key has been revoked", 401));
    }
    if (match.expires_at && match.expires_at < new Date()) {
      return next(new AppError("This API key has expired", 401));
    }

    req.apiKey = {
      keyId: match.id,
      teamId: match.team_id,
      scopes: new Set(match.scopes),
    };

    // Don't block the request on this - last_used_at is best-effort.
    touchLastUsed(match.id).catch(() => {});

    return next();
  } catch (err) {
    return next(err);
  }
}

export function requireScope(scope) {
  return function (req, res, next) {
    if (!req.apiKey?.scopes.has(scope)) {
      return next(new AppError("This API key is missing a required scope", 403));
    }
    return next();
  };
}
