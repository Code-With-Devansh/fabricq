import express from "express";
import { authenticateJWT } from "../middlewares/authenticateJWT.js";
import { loadTeamContext, requirePermission } from "../middlewares/teamContext.js";
import {
  listMyTeams,
  getTeam,
  updateTeam,
  deleteTeam,
  listMembers,
  updateMemberRole,
  removeMember,
  listPermissionsCatalog,
  listRoles,
  createRole,
  deleteRole,
} from "../controller/team.controller.js";

const router = express.Router();

router.use(authenticateJWT);

// Static reference data - every authenticated user can see what
// permission keys exist, no team context needed.
router.get("/permissions/catalog", listPermissionsCatalog);

// Teams the caller belongs to (dashboard team switcher).
router.get("/", listMyTeams);

const team = loadTeamContext("teamId");

router.get("/:teamId", team, requirePermission("team:read"), getTeam);
router.patch("/:teamId", team, requirePermission("team:update"), updateTeam);
// No requirePermission gate here - "delete team" is enforced as
// OWNER-only inside the service, deliberately not a grantable catalog
// permission (see migration 014 notes).
router.delete("/:teamId", team, deleteTeam);

router.get(
  "/:teamId/members",
  team,
  requirePermission("members:read"),
  listMembers
);
router.patch(
  "/:teamId/members/:userId",
  team,
  requirePermission("members:update"),
  updateMemberRole
);
router.delete(
  "/:teamId/members/:userId",
  team,
  requirePermission("members:remove"),
  removeMember
);

router.get("/:teamId/roles", team, requirePermission("members:read"), listRoles);
router.post(
  "/:teamId/roles",
  team,
  requirePermission("members:update"),
  createRole
);
router.delete(
  "/:teamId/roles/:roleId",
  team,
  requirePermission("members:update"),
  deleteRole
);

export default router;
