import { AppError } from "../Error/appError.js";
import { findMembership } from "../repositories/membership.repository.js";
import { getPermissionsForRole } from "../repositories/role.repository.js";

/**
 * Resolves req.auth.userId's membership + permissions for req.params.teamId
 * and attaches req.team = { teamId, membershipId, userId, roleId, roleName,
 * roleIsSystem, permissions: Set<string> }.
 *
 * Must run after authenticateJWT. 404s (not 403) when the user has no
 * membership on the team, so team existence/membership isn't leaked to
 * non-members.
 */
export function loadTeamContext(paramName = "teamId") {
  return async function (req, res, next) {
    const teamId = req.params[paramName];

    try {
      const membership = await findMembership(teamId, req.auth.userId);
      if (!membership) {
        return next(new AppError("Team not found", 404));
      }

      const permissionKeys = await getPermissionsForRole(membership.role_id);

      req.team = {
        teamId,
        membershipId: membership.id,
        userId: membership.user_id,
        roleId: membership.role_id,
        roleName: membership.role_name,
        roleIsSystem: membership.role_is_system,
        permissions: new Set(permissionKeys),
      };

      return next();
    } catch (err) {
      return next(err);
    }
  };
}

/**
 * Requires req.team.permissions to contain `permission`. Must run after
 * loadTeamContext.
 */
export function requirePermission(permission) {
  return function (req, res, next) {
    if (!req.team?.permissions.has(permission)) {
      return next(new AppError("Insufficient permissions", 403));
    }
    return next();
  };
}
