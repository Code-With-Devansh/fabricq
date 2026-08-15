import express from "express";
import { authenticateJWT } from "../middlewares/authenticateJWT.js";
import { loadTeamContext, requirePermission } from "../middlewares/teamContext.js";
import jobsRoute from "./jobs.route.js";
import apiKeyRoute from "./apiKey.route.js";
import {
  listMyTeams,
  createTeam,
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
  createInvitation,
  listInvitations,
  revokeInvitation,
  previewInvitation,
  acceptInvitation,
} from "../controller/team.controller.js";

const router = express.Router();

// Public - unauthenticated preview of an invite, so a frontend can show
// "You've been invited to join <team>" before login/signup. Mounted
// before authenticateJWT deliberately.
router.get("/invitations/preview/:token", previewInvitation);

router.use(authenticateJWT);

// Static reference data - every authenticated user can see what
// permission keys exist, no team context needed.
router.get("/permissions/catalog", listPermissionsCatalog);

// Teams the caller belongs to (dashboard team switcher).
router.get("/", listMyTeams);
// Any logged-in user can spin up an additional team - they become its
// OWNER. This is how a user ends up on more than one team, alongside
// accepting invitations into teams other people created.
router.post("/", createTeam);

// Accepting an invite only needs the caller's identity, not membership
// on the target team yet - so this sits outside loadTeamContext.
router.post("/invitations/accept", acceptInvitation);

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

router.get(
  "/:teamId/invitations",
  team,
  requirePermission("members:invite"),
  listInvitations
);
router.post(
  "/:teamId/invitations",
  team,
  requirePermission("members:invite"),
  createInvitation
);
router.delete(
  "/:teamId/invitations/:invitationId",
  team,
  requirePermission("members:invite"),
  revokeInvitation
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

// Jobs and API keys apply their own authenticateJWT + loadTeamContext
// internally (see jobs.route.js / apiKey.route.js) so each sub-router is
// self-contained; the public API equivalent lives separately at
// routes/v1/jobs.route.js, authenticated by API key instead of JWT.
router.use("/:teamId/jobs", jobsRoute);
router.use("/:teamId/api-keys", apiKeyRoute);

export default router;
