import { pool } from "../config/db.js";

export async function createInvitation(
  client,
  { teamId, email, roleId, tokenHash, invitedBy, expiresAt }
) {
  const { rows } = await client.query(
    `INSERT INTO team_invitations
       (team_id, email, role_id, token_hash, invited_by, expires_at)
     VALUES ($1, $2, $3, $4, $5, $6)
     RETURNING *`,
    [teamId, email, roleId, tokenHash, invitedBy, expiresAt]
  );
  return rows[0];
}

// Used by createInvitationService to atomically replace a still-pending
// invite for the same (team, email) - see idx_team_invitations_pending_unique.
export async function revokePendingInvitationForEmail(client, { teamId, email }) {
  await client.query(
    `UPDATE team_invitations
     SET status = 'revoked'
     WHERE team_id = $1 AND email = $2 AND status = 'pending'`,
    [teamId, email]
  );
}

export async function findInvitationById(teamId, invitationId) {
  const { rows } = await pool.query(
    `SELECT ti.*, r.name AS role_name
     FROM team_invitations ti
     JOIN roles r ON r.id = ti.role_id
     WHERE ti.team_id = $1 AND ti.id = $2`,
    [teamId, invitationId]
  );
  return rows[0] ?? null;
}

export async function findInvitationByTokenHash(tokenHash) {
  const { rows } = await pool.query(
    `SELECT ti.*, r.name AS role_name, t.name AS team_name
     FROM team_invitations ti
     JOIN roles r ON r.id = ti.role_id
     JOIN teams t ON t.id = ti.team_id
     WHERE ti.token_hash = $1`,
    [tokenHash]
  );
  return rows[0] ?? null;
}

export async function listPendingInvitationsForTeam(teamId) {
  const { rows } = await pool.query(
    `SELECT ti.id, ti.email, ti.status, ti.expires_at, ti.created_at,
            r.id AS role_id, r.name AS role_name,
            u.id AS invited_by_id, u.email AS invited_by_email
     FROM team_invitations ti
     JOIN roles r ON r.id = ti.role_id
     LEFT JOIN users u ON u.id = ti.invited_by
     WHERE ti.team_id = $1 AND ti.status = 'pending'
     ORDER BY ti.created_at DESC`,
    [teamId]
  );
  return rows;
}

export async function markInvitationAccepted(client, { invitationId, userId }) {
  const { rows } = await client.query(
    `UPDATE team_invitations
     SET status = 'accepted', accepted_at = now(), accepted_by = $2
     WHERE id = $1
     RETURNING *`,
    [invitationId, userId]
  );
  return rows[0] ?? null;
}

export async function revokeInvitation(client, { teamId, invitationId }) {
  const { rows } = await client.query(
    `UPDATE team_invitations
     SET status = 'revoked'
     WHERE id = $1 AND team_id = $2 AND status = 'pending'
     RETURNING *`,
    [invitationId, teamId]
  );
  return rows[0] ?? null;
}
