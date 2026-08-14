import { pool } from "../config/db.js";
import { AppError } from "../Error/appError.js";
import { generateApiKey } from "../utils/apiKey.js";
import {
  createApiKey,
  listForTeam,
  findByIdForTeam,
  revokeApiKey,
} from "../repositories/apiKey.repository.js";

// Machine permission catalog - deliberately narrower than the human one.
// A key can act on jobs/executions; it can never manage the team, other
// keys, or members, no matter how permissive its creator's role is.
export const API_KEY_SCOPES = [
  "jobs:read",
  "jobs:write",
  "jobs:delete",
  "executions:read",
];

export async function listApiKeysService(teamId) {
  return listForTeam(teamId);
}

/**
 * Creates a key scoped to `requestedScopes`, but never beyond what the
 * creating user's own resolved team permissions already grant - otherwise
 * a scoped API key becomes a privilege-escalation path around the human
 * role hierarchy (e.g. an ADMIN without jobs:delete minting a key that
 * has it).
 */
export async function createApiKeyService({
  teamId,
  name,
  requestedScopes,
  expiresAt,
  createdBy,
  creatorPermissions, // Set<string> - req.team.permissions
}) {
  const invalid = requestedScopes.filter((s) => !API_KEY_SCOPES.includes(s));
  if (invalid.length > 0) {
    throw new AppError(`Unknown scope(s): ${invalid.join(", ")}`, 400);
  }

  const exceeds = requestedScopes.filter((s) => !creatorPermissions.has(s));
  if (exceeds.length > 0) {
    throw new AppError(
      `Cannot grant scope(s) you don't have yourself: ${exceeds.join(", ")}`,
      403
    );
  }

  const { raw, prefix, hash } = generateApiKey();

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const row = await createApiKey(client, {
      teamId,
      name,
      prefix,
      hash,
      scopes: requestedScopes,
      expiresAt: expiresAt ?? null,
      createdBy,
    });
    await client.query("COMMIT");

    // Raw key is only ever available here, at creation time.
    return { ...row, api_key: raw };
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

export async function revokeApiKeyService({ teamId, keyId }) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const revoked = await revokeApiKey(client, { teamId, keyId });
    await client.query("COMMIT");
    if (!revoked) throw new AppError("API key not found", 404);
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

export { findByIdForTeam as findApiKeyByIdForTeam };
