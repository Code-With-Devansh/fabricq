import { AppError } from "../Error/appError.js";
import { findMembership } from "../repositories/membership.repository.js";
import {
  getCachedTeamContext,
  setCachedTeamContext,
} from "../cache/teamContextCache.js";

/**
 * Resolves req.auth.userId's membership + permissions for req.params.teamId
 * and attaches req.team = { teamId, membershipId, userId, roleId, roleName,
 * roleIsSystem, permissions: Set<string> }.
 *
 * Must run after authenticateJWT. 404s (not 403) when the user has no
 * membership on the team, so team existence/membership isn't leaked to
 * non-members.
 *
 * Cached in Redis per (teamId, userId), including negative caching - a
 * user with no membership on this team gets that "not found" result
 * cached too, so a wrong/stale team_id (or a just-removed member) isn't
 * a free Postgres query on every retry. This runs on nearly every
 * dashboard request, same hot-path shape as authenticateApiKey's prefix
 * lookup, so it gets the same treatment: cache first, single joined DB
 * query (membership + role + permissions in one round trip) on a miss,
 * explicit invalidation on role change / removal / new membership (see
 * team.service.js).
 */
export function loadTeamContext(paramName = "teamId") {
  return async function (req, res, next) {
    const teamId = req.params[paramName];
    const userId = req.auth.userId;

    try {
      let ctx = await getCachedTeamContext(teamId, userId);

      if (ctx === undefined) {
        const membership = await findMembership(teamId, userId);

        ctx = membership
          ? {
              membershipId: membership.id,
              userId: membership.user_id,
              roleId: membership.role_id,
              roleName: membership.role_name,
              roleIsSystem: membership.role_is_system,
              permissionKeys: membership.permission_keys,
            }
          : null;

        // Don't block the request on this - same fire-and-forget
        // reasoning as markKeyUsed in the API-key path. Caches the
        // not-found (null) result too, same as a hit.
        setCachedTeamContext(teamId, userId, ctx).catch(() => {});
      }

      if (ctx === null) {
        return next(new AppError("Team not found", 404));
      }

      req.team = {
        teamId,
        membershipId: ctx.membershipId,
        userId: ctx.userId,
        roleId: ctx.roleId,
        roleName: ctx.roleName,
        roleIsSystem: ctx.roleIsSystem,
        permissions: new Set(ctx.permissionKeys),
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
