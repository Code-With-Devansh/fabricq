import { pool } from "../config/db.js";

export async function createMembership(client, { teamId, userId, roleId }) {
  const { rows } = await client.query(
    `INSERT INTO team_memberships (team_id, user_id, role_id)
     VALUES ($1, $2, $3)
     RETURNING *`,
    [teamId, userId, roleId]
  );
  return rows[0];
}

// Joins in the role name + is_system flag, which every permission/
// hierarchy check needs alongside the raw role_id.
export async function findMembership(teamId, userId) {
  const { rows } = await pool.query(
    `SELECT tm.id, tm.team_id, tm.user_id, tm.role_id,
            r.name AS role_name, r.is_system AS role_is_system
     FROM team_memberships tm
     JOIN roles r ON r.id = tm.role_id
     WHERE tm.team_id = $1 AND tm.user_id = $2`,
    [teamId, userId]
  );
  return rows[0] ?? null;
}

export async function listMembersForTeam(teamId) {
  const { rows } = await pool.query(
    `SELECT tm.id AS membership_id, u.id AS user_id, u.email,
            r.id AS role_id, r.name AS role_name, r.is_system AS role_is_system,
            tm.created_at
     FROM team_memberships tm
     JOIN users u ON u.id = tm.user_id
     JOIN roles r ON r.id = tm.role_id
     WHERE tm.team_id = $1
     ORDER BY tm.created_at ASC`,
    [teamId]
  );
  return rows;
}

export async function countOwners(teamId) {
  const { rows } = await pool.query(
    `SELECT COUNT(*)::int AS count
     FROM team_memberships tm
     JOIN roles r ON r.id = tm.role_id
     WHERE tm.team_id = $1 AND r.name = 'OWNER' AND r.is_system`,
    [teamId]
  );
  return rows[0].count;
}

export async function updateMembershipRole(client, { membershipId, roleId }) {
  const { rows } = await client.query(
    `UPDATE team_memberships SET role_id = $2 WHERE id = $1 RETURNING *`,
    [membershipId, roleId]
  );
  return rows[0] ?? null;
}

export async function deleteMembership(client, membershipId) {
  await client.query(`DELETE FROM team_memberships WHERE id = $1`, [
    membershipId,
  ]);
}
