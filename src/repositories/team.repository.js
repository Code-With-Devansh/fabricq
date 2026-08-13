import { pool } from "../config/db.js";

export async function createTeam(client, { name }) {
  const { rows } = await client.query(
    `INSERT INTO teams (name) VALUES ($1) RETURNING *`,
    [name]
  );
  return rows[0];
}

export async function findTeamById(teamId) {
  const { rows } = await pool.query(`SELECT * FROM teams WHERE id = $1`, [
    teamId,
  ]);
  return rows[0] ?? null;
}

export async function updateTeamName(client, { teamId, name }) {
  const { rows } = await client.query(
    `UPDATE teams SET name = $2 WHERE id = $1 RETURNING *`,
    [teamId, name]
  );
  return rows[0] ?? null;
}

export async function deleteTeam(client, teamId) {
  await client.query(`DELETE FROM teams WHERE id = $1`, [teamId]);
}

export async function listTeamsForUser(userId) {
  const { rows } = await pool.query(
    `SELECT t.id, t.name, t.created_at, r.id AS role_id, r.name AS role_name
     FROM team_memberships tm
     JOIN teams t ON t.id = tm.team_id
     JOIN roles r ON r.id = tm.role_id
     WHERE tm.user_id = $1
     ORDER BY t.created_at ASC`,
    [userId]
  );
  return rows;
}
