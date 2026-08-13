// Trust ranking used only for member-management guards (who can change/
// remove whose membership) - separate from the permission catalog, which
// governs what actions a role can take at all.
//
// Custom roles rank at MEMBER level: they're team-defined and scoped to
// day-to-day permissions (e.g. "Developer": jobs:read/write), not part of
// the ownership/admin trust chain, so an admin can manage members holding
// a custom role but never another admin or the owner.
const RANK = { OWNER: 3, ADMIN: 2, MEMBER: 1 };

export function roleRank({ roleName, roleIsSystem }) {
  if (roleIsSystem && roleName in RANK) return RANK[roleName];
  return RANK.MEMBER;
}

// True if `actor` is allowed to change or remove `target`'s membership.
// - Owner can manage anyone (subject to the "last owner" guard elsewhere).
// - Admin can manage members/custom-role holders, but never another
//   admin or the owner.
// - Member (or below) can manage no one.
export function canManageMembership(actor, target) {
  const actorRank = roleRank(actor);
  const targetRank = roleRank(target);

  if (actorRank === RANK.OWNER) return true;
  if (actorRank === RANK.ADMIN) return targetRank < RANK.ADMIN;
  return false;
}
