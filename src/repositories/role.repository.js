import { pool } from "../config/db.js";

export const SYSTEM_ROLE = {
  OWNER: "00000000-0000-0000-0000-000000000001",
  ADMIN: "00000000-0000-0000-0000-000000000002",
  MEMBER: "00000000-0000-0000-0000-000000000003",
};

export async function listAllPermissions() {
  const { rows } = await pool.query(
    `SELECT key, description FROM permissions ORDER BY key`
  );
  return rows;
}

export async function findRoleById(roleId) {
  const { rows } = await pool.query(`SELECT * FROM roles WHERE id = $1`, [
    roleId,
  ]);
  return rows[0] ?? null;
}

// Roles a team can assign: the three system roles + any roles it has
// created itself.
export async function listRolesForTeam(teamId) {
  const { rows } = await pool.query(
    `SELECT id, team_id, name, is_system, created_at
     FROM roles
     WHERE is_system OR team_id = $1
     ORDER BY is_system DESC, name ASC`,
    [teamId]
  );
  return rows;
}

export async function getPermissionsForRole(roleId) {
  const { rows } = await pool.query(
    `SELECT permission_key FROM role_permissions WHERE role_id = $1`,
    [roleId]
  );
  return rows.map((r) => r.permission_key);
}

export async function createCustomRole(
  client,
  { teamId, name, permissionKeys }
) {
  const { rows } = await client.query(
    `INSERT INTO roles (team_id, name, is_system) VALUES ($1, $2, FALSE) RETURNING *`,
    [teamId, name]
  );
  const role = rows[0];

  if (permissionKeys.length > 0) {
    const values = permissionKeys
      .map((_, i) => `($1, $${i + 2})`)
      .join(", ");
    await client.query(
      `INSERT INTO role_permissions (role_id, permission_key) VALUES ${values}`,
      [role.id, ...permissionKeys]
    );
  }

  return role;
}

export async function deleteCustomRole(client, { teamId, roleId }) {
  // Scoped by team_id so one team can't delete another team's custom role.
  await client.query(
    `DELETE FROM roles WHERE id = $1 AND team_id = $2 AND NOT is_system`,
    [roleId, teamId]
  );
}
