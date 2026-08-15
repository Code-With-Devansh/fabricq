import { asyncHandler } from "../middlewares/asyncHandler.js";
import { AppError } from "../Error/appError.js";
import {
  createTeamSchema,
  updateTeamSchema,
  updateMemberRoleSchema,
  createRoleSchema,
  createInvitationSchema,
  acceptInvitationSchema,
} from "../validators/team.js";
import {
  listMyTeamsService,
  createTeamService,
  getTeamService,
  updateTeamService,
  deleteTeamService,
  listMembersService,
  updateMemberRoleService,
  removeMemberService,
  listPermissionsCatalogService,
  listRolesService,
  createCustomRoleService,
  deleteCustomRoleService,
  createInvitationService,
  listInvitationsService,
  revokeInvitationService,
  previewInvitationService,
  acceptInvitationService,
} from "../services/team.service.js";

export const listMyTeams = asyncHandler(async (req, res) => {
  const teams = await listMyTeamsService(req.auth.userId);
  return res.status(200).json({ success: true, data: teams });
});

// Any logged-in user can create a new team - they become its OWNER.
// This is how a user ends up belonging to more than one team, alongside
// accepting invitations into teams created by others.
export const createTeam = asyncHandler(async (req, res) => {
  const validated = createTeamSchema.safeParse(req.body);
  if (!validated.success) {
    throw new AppError("Validation failed", 400, validated.error.flatten());
  }

  const team = await createTeamService({
    userId: req.auth.userId,
    name: validated.data.name,
  });
  return res.status(201).json({ success: true, data: team });
});

export const getTeam = asyncHandler(async (req, res) => {
  const team = await getTeamService(req.team.teamId);
  return res.status(200).json({
    success: true,
    data: { ...team, my_role: req.team.roleName },
  });
});

export const updateTeam = asyncHandler(async (req, res) => {
  const validated = updateTeamSchema.safeParse(req.body);
  if (!validated.success) {
    throw new AppError("Validation failed", 400, validated.error.flatten());
  }

  const team = await updateTeamService({
    teamId: req.team.teamId,
    name: validated.data.name,
  });
  return res.status(200).json({ success: true, data: team });
});

export const deleteTeam = asyncHandler(async (req, res) => {
  await deleteTeamService({
    teamId: req.team.teamId,
    actorRoleName: req.team.roleName,
    actorRoleIsSystem: req.team.roleIsSystem,
  });
  return res.status(204).send();
});

export const listMembers = asyncHandler(async (req, res) => {
  const members = await listMembersService(req.team.teamId);
  return res.status(200).json({ success: true, data: members });
});

export const updateMemberRole = asyncHandler(async (req, res) => {
  const validated = updateMemberRoleSchema.safeParse(req.body);
  if (!validated.success) {
    throw new AppError("Validation failed", 400, validated.error.flatten());
  }

  const updated = await updateMemberRoleService({
    teamId: req.team.teamId,
    targetUserId: req.params.userId,
    newRoleId: validated.data.role_id,
    actor: { roleName: req.team.roleName, roleIsSystem: req.team.roleIsSystem },
  });
  return res.status(200).json({ success: true, data: updated });
});

export const removeMember = asyncHandler(async (req, res) => {
  await removeMemberService({
    teamId: req.team.teamId,
    targetUserId: req.params.userId,
    actor: { roleName: req.team.roleName, roleIsSystem: req.team.roleIsSystem },
  });
  return res.status(204).send();
});

export const listPermissionsCatalog = asyncHandler(async (req, res) => {
  const permissions = await listPermissionsCatalogService();
  return res.status(200).json({ success: true, data: permissions });
});

export const listRoles = asyncHandler(async (req, res) => {
  const roles = await listRolesService(req.team.teamId);
  return res.status(200).json({ success: true, data: roles });
});

export const createRole = asyncHandler(async (req, res) => {
  const validated = createRoleSchema.safeParse(req.body);
  if (!validated.success) {
    throw new AppError("Validation failed", 400, validated.error.flatten());
  }

  const role = await createCustomRoleService({
    teamId: req.team.teamId,
    name: validated.data.name,
    permissionKeys: validated.data.permissions,
  });
  return res.status(201).json({ success: true, data: role });
});

export const deleteRole = asyncHandler(async (req, res) => {
  await deleteCustomRoleService({
    teamId: req.team.teamId,
    roleId: req.params.roleId,
  });
  return res.status(204).send();
});

// --- invitations -------------------------------------------------------

export const createInvitation = asyncHandler(async (req, res) => {
  const validated = createInvitationSchema.safeParse(req.body);
  if (!validated.success) {
    throw new AppError("Validation failed", 400, validated.error.flatten());
  }

  const { invitation } = await createInvitationService({
    teamId: req.team.teamId,
    email: validated.data.email,
    roleId: validated.data.role_id,
    invitedBy: req.auth.userId,
  });

  // The raw token is only used to build the accept link that was just
  // emailed via the mail queue - it's not returned here, mirroring how
  // refresh tokens/API keys aren't echoed back outside their one-time
  // issuance path.
  return res.status(201).json({
    success: true,
    data: {
      id: invitation.id,
      email: invitation.email,
      role_id: invitation.role_id,
      expires_at: invitation.expires_at,
      status: invitation.status,
    },
  });
});

export const listInvitations = asyncHandler(async (req, res) => {
  const invitations = await listInvitationsService(req.team.teamId);
  return res.status(200).json({ success: true, data: invitations });
});

export const revokeInvitation = asyncHandler(async (req, res) => {
  await revokeInvitationService({
    teamId: req.team.teamId,
    invitationId: req.params.invitationId,
  });
  return res.status(204).send();
});

// Public - no auth. Lets a frontend show who/what the invite is for
// before the person logs in or signs up.
export const previewInvitation = asyncHandler(async (req, res) => {
  const preview = await previewInvitationService(req.params.token);
  return res.status(200).json({ success: true, data: preview });
});

// Requires auth - the logged-in user's email must match the invite.
export const acceptInvitation = asyncHandler(async (req, res) => {
  const validated = acceptInvitationSchema.safeParse(req.body);
  if (!validated.success) {
    throw new AppError("Validation failed", 400, validated.error.flatten());
  }

  const result = await acceptInvitationService({
    token: validated.data.token,
    userId: req.auth.userId,
  });
  return res.status(200).json({ success: true, data: result });
});
