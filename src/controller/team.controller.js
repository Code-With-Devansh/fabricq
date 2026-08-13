import { asyncHandler } from "../middlewares/asyncHandler.js";
import { AppError } from "../Error/appError.js";
import {
  updateTeamSchema,
  updateMemberRoleSchema,
  createRoleSchema,
} from "../validators/team.js";
import {
  listMyTeamsService,
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
} from "../services/team.service.js";

export const listMyTeams = asyncHandler(async (req, res) => {
  const teams = await listMyTeamsService(req.auth.userId);
  return res.status(200).json({ success: true, data: teams });
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
