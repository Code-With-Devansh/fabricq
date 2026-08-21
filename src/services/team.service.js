import { pool } from "../config/db.js";
import { AppError } from "../Error/appError.js";
import config from "../config/index.js";
import {
  createTeam as createTeamRow,
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
  createMembership,
} from "../repositories/membership.repository.js";
import {
  SYSTEM_ROLE,
  listAllPermissions,
  listRolesForTeam,
  findRoleById,
  createCustomRole as createCustomRoleRow,
  deleteCustomRole as deleteCustomRoleRow,
} from "../repositories/role.repository.js";
import {
  createInvitation,
  revokePendingInvitationForEmail,
  findInvitationById,
  findInvitationByTokenHash,
  listPendingInvitationsForTeam,
  markInvitationAccepted,
  revokeInvitation as revokeInvitationRow,
} from "../repositories/invitation.repository.js";
import { canManageMembership } from "../utils/roleHierarchy.js";
import { invalidateTeamContextCache } from "../cache/teamContextCache.js";
import { generateInviteToken, hashToken } from "../utils/tokens.js";
import { findUserById } from "../repositories/auth.repository.js";
import { enqueueInvitationEmail } from "../queues/mail.queue.js";

export async function listMyTeamsService(userId) {
  const rows = await listTeamsForUser(userId);
  return rows.map((r) => ({
    id: r.id,
    name: r.name,
    created_at: r.created_at,
    role: r.role_name,
  }));
}

/**
 * A logged-in user creating an additional team (distinct from signup,
 * which creates a user's first team). Same shape as the signup path:
 * new team + caller as its OWNER, in one transaction.
 */
export async function createTeamService({ userId, name }) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const team = await createTeamRow(client, { name });
    await createMembership(client, {
      teamId: team.id,
      userId,
      roleId: SYSTEM_ROLE.OWNER,
    });
    await client.query("COMMIT");
    return { ...team, role: "OWNER" };
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
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
    // Role changed - drop the cached permission set so the demotion/
    // promotion takes effect on this member's very next request instead
    // of waiting out the TTL.
    await invalidateTeamContextCache(teamId, targetUserId);
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
    // Removed member shouldn't keep passing loadTeamContext off a stale
    // cache entry for up to the TTL - drop it immediately.
    await invalidateTeamContextCache(teamId, targetUserId);
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

// --- invitations -----------------------------------------------------

/**
 * Creates (or replaces) a pending invitation for `email` on `teamId`,
 * then enqueues a BullMQ job (queue: "email") so the mail worker process
 * sends the actual invite email via Resend. The API call returns as soon
 * as the job is enqueued - it doesn't wait on the send itself.
 */
export async function createInvitationService({
  teamId,
  email,
  roleId,
  invitedBy,
}) {
  const role = await findRoleById(roleId);
  if (!role || (role.team_id && role.team_id !== teamId)) {
    throw new AppError("Role not found", 404);
  }

  const existingMembership = await findMembershipByEmail(teamId, email);
  if (existingMembership) {
    throw new AppError("This person is already a member of the team", 409);
  }

  const [team, inviter] = await Promise.all([
    findTeamById(teamId),
    findUserById(invitedBy),
  ]);
  if (!team) throw new AppError("Team not found", 404);

  const { raw, hash } = generateInviteToken();
  const expiresAt = new Date(
    Date.now() + config.auth.inviteTokenTtlSeconds * 1000
  );

  const client = await pool.connect();
  let invitation;
  try {
    await client.query("BEGIN");
    // Replace any still-pending invite to this email first - the
    // partial unique index (team_id, email) WHERE status='pending'
    // would otherwise reject the insert on a re-invite.
    await revokePendingInvitationForEmail(client, { teamId, email });
    invitation = await createInvitation(client, {
      teamId,
      email,
      roleId,
      tokenHash: hash,
      invitedBy,
      expiresAt,
    });
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }

  const acceptUrl = `${config.dashboardOrigin}/invitations/${raw}`;

  await enqueueInvitationEmail({
    invitationId: invitation.id,
    teamName: team.name,
    inviterEmail: inviter?.email ?? "A team admin",
    roleName: role.name,
    toEmail: invitation.email,
    acceptUrl,
    expiresAt: invitation.expires_at,
  });

  return { invitation, token: raw };
}

export async function listInvitationsService(teamId) {
  return listPendingInvitationsForTeam(teamId);
}

export async function revokeInvitationService({ teamId, invitationId }) {
  const revoked = await withTransaction((client) =>
    revokeInvitationRow(client, { teamId, invitationId })
  );
  if (!revoked) {
    throw new AppError("Pending invitation not found", 404);
  }
}

/**
 * Public preview (no auth) so a frontend can show "You've been invited
 * to join <team> as <role>" before the person logs in or signs up.
 * Deliberately returns nothing else about the team.
 */
export async function previewInvitationService(token) {
  const invitation = await findInvitationByToken(token);
  return {
    team_name: invitation.team_name,
    role: invitation.role_name,
    email: invitation.email,
    expires_at: invitation.expires_at,
  };
}

/**
 * Accepting requires the caller to be logged in as the invited email -
 * otherwise anyone who intercepts the link could join a team meant for
 * someone else. Idempotent against double-accept via invitation.status.
 */
export async function acceptInvitationService({ token, userId }) {
  const invitation = await findInvitationByToken(token);

  // The access token only carries userId (see utils/jwt.js), so the
  // invited-email check is done against the current DB row, not a
  // claim baked into the token.
  const user = await findUserById(userId);
  if (!user) throw new AppError("User not found", 404);

  if (invitation.email.toLowerCase() !== user.email.toLowerCase()) {
    throw new AppError(
      "This invitation was sent to a different email address",
      403
    );
  }

  const existingMembership = await findMembership(invitation.team_id, userId);
  if (existingMembership) {
    throw new AppError("You're already a member of this team", 409);
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await createMembership(client, {
      teamId: invitation.team_id,
      userId,
      roleId: invitation.role_id,
    });
    await markInvitationAccepted(client, {
      invitationId: invitation.id,
      userId,
    });
    await client.query("COMMIT");
    // Clears any cached "not found" from a prior request against this
    // team (e.g. the invite preview page, or a stray dashboard call)
    // made before the membership existed.
    await invalidateTeamContextCache(invitation.team_id, userId);
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }

  return { teamId: invitation.team_id, teamName: invitation.team_name, role: invitation.role_name };
}

// --- invitation helpers -----------------------------------------------

async function findInvitationByToken(token) {
  const invitation = await findInvitationByTokenHash(hashToken(token));

  if (!invitation) throw new AppError("Invitation not found", 404);
  if (invitation.status === "accepted") {
    throw new AppError("This invitation has already been accepted", 409);
  }
  if (invitation.status === "revoked") {
    throw new AppError("This invitation is no longer valid", 410);
  }
  if (invitation.expires_at < new Date()) {
    throw new AppError("This invitation has expired", 410);
  }
  return invitation;
}

// Membership-by-email is only needed here (invite-time dedupe), so it's
// a small local helper on top of the existing member list rather than a
// new indexed repository query.
async function findMembershipByEmail(teamId, email) {
  const members = await listMembersForTeam(teamId);
  return members.find((m) => m.email.toLowerCase() === email.toLowerCase());
}

async function withTransaction(fn) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const result = await fn(client);
    await client.query("COMMIT");
    return result;
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

export { SYSTEM_ROLE };
