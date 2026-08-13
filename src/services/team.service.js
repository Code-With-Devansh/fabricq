import { pool } from "../config/db.js";
import { AppError } from "../Error/appError.js";
import {
  findTeamById,
  updateTeamName,
  deleteTeam as deleteTeamRow,
  listTeamsForUser,
} from "../repositories/team.repository.js";
import {
  findMembership,
  listMembersForTeam,
  countOwners,
  updateMembershipRole,
  deleteMembership,
} from "../repositories/membership.repository.js";
import {
  SYSTEM_ROLE,
  listAllPermissions,
  listRolesForTeam,
  findRoleById,
  createCustomRole as createCustomRoleRow,
  deleteCustomRole as deleteCustomRoleRow,
} from "../repositories/role.repository.js";
import { canManageMembership } from "../utils/roleHierarchy.js";

export async function listMyTeamsService(userId) {
  const rows = await listTeamsForUser(userId);
  return rows.map((r) => ({
    id: r.id,
    name: r.name,
    created_at: r.created_at,
    role: r.role_name,
  }));
}

export async function getTeamService(teamId) {
  const team = await findTeamById(teamId);
  if (!team) throw new AppError("Team not found", 404);
  return team;
}

export async function updateTeamService({ teamId, name }) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const team = await updateTeamName(client, { teamId, name });
    await client.query("COMMIT");
    if (!team) throw new AppError("Team not found", 404);
    return team;
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

/**
 * Only the OWNER role may delete a team - this is deliberately not a
 * catalog permission (see migration 014 notes): it's a one-time
 * destructive action tied to sole ownership, not something an admin
 * should ever be grantable.
 */
export async function deleteTeamService({ teamId, actorRoleName, actorRoleIsSystem }) {
  if (!(actorRoleIsSystem && actorRoleName === "OWNER")) {
    throw new AppError("Only the team owner can delete the team", 403);
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await deleteTeamRow(client, teamId);
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

export async function listMembersService(teamId) {
  const rows = await listMembersForTeam(teamId);
  return rows.map((r) => ({
    membership_id: r.membership_id,
    user_id: r.user_id,
    email: r.email,
    role: { id: r.role_id, name: r.role_name, is_system: r.role_is_system },
    joined_at: r.created_at,
  }));
}

/**
 * Changes a member's role, enforcing:
 *  - hierarchy (admins can't touch admins/owner - see canManageMembership)
 *  - at least one OWNER must remain on the team at all times
 */
export async function updateMemberRoleService({
  teamId,
  targetUserId,
  newRoleId,
  actor, // { roleName, roleIsSystem }
}) {
  const targetMembership = await findMembership(teamId, targetUserId);
  if (!targetMembership) throw new AppError("Member not found", 404);

  const target = {
    roleName: targetMembership.role_name,
    roleIsSystem: targetMembership.role_is_system,
  };

  if (!canManageMembership(actor, target)) {
    throw new AppError(
      "You don't have permission to change this member's role",
      403
    );
  }

  const newRole = await findRoleById(newRoleId);
  if (!newRole || (newRole.team_id && newRole.team_id !== teamId)) {
    throw new AppError("Role not found", 404);
  }

  const isDemotionFromOwner =
    target.roleIsSystem &&
    target.roleName === "OWNER" &&
    newRole.name !== "OWNER";

  if (isDemotionFromOwner) {
    const owners = await countOwners(teamId);
    if (owners <= 1) {
      throw new AppError(
        "Cannot demote the last owner - promote another member to owner first",
        409
      );
    }
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const updated = await updateMembershipRole(client, {
      membershipId: targetMembership.id,
      roleId: newRoleId,
    });
    await client.query("COMMIT");
    return updated;
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

export async function removeMemberService({ teamId, targetUserId, actor }) {
  const targetMembership = await findMembership(teamId, targetUserId);
  if (!targetMembership) throw new AppError("Member not found", 404);

  const target = {
    roleName: targetMembership.role_name,
    roleIsSystem: targetMembership.role_is_system,
  };

  if (!canManageMembership(actor, target)) {
    throw new AppError(
      "You don't have permission to remove this member",
      403
    );
  }

  if (target.roleIsSystem && target.roleName === "OWNER") {
    const owners = await countOwners(teamId);
    if (owners <= 1) {
      throw new AppError(
        "Cannot remove the last owner - promote another member to owner first",
        409
      );
    }
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await deleteMembership(client, targetMembership.id);
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

// --- roles ----------------------------------------------------------

export async function listPermissionsCatalogService() {
  return listAllPermissions();
}

export async function listRolesService(teamId) {
  return listRolesForTeam(teamId);
}

export async function createCustomRoleService({ teamId, name, permissionKeys }) {
  const catalog = await listAllPermissions();
  const validKeys = new Set(catalog.map((p) => p.key));
  const invalid = permissionKeys.filter((k) => !validKeys.has(k));
  if (invalid.length > 0) {
    throw new AppError(`Unknown permission key(s): ${invalid.join(", ")}`, 400);
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const role = await createCustomRoleRow(client, {
      teamId,
      name,
      permissionKeys,
    });
    await client.query("COMMIT");
    return role;
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

export async function deleteCustomRoleService({ teamId, roleId }) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await deleteCustomRoleRow(client, { teamId, roleId });
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

export { SYSTEM_ROLE };
