-- Phase 1.5: accounts -> teams, plus a two-axis role/permission system.
--
-- A user's role is no longer a global property (users.role) - it's a
-- property of the (user, team) pair, since one user can belong to many
-- teams with a different role in each. Jobs and API keys (phase 2) will
-- belong to a team_id, never a user_id.

ALTER TABLE accounts RENAME TO teams;

-- --------------------------------------------------------------------
-- Permission catalog. Fixed set of human (dashboard) permission keys.
-- API-key scopes (machine permissions) are a separate system, phase 2.
-- --------------------------------------------------------------------
CREATE TABLE permissions (
    key         TEXT PRIMARY KEY,
    description TEXT NOT NULL
);

INSERT INTO permissions (key, description) VALUES
    ('team:read',       'View team details'),
    ('team:update',     'Rename/update team settings'),
    ('members:read',    'View team members'),
    ('members:invite',  'Invite new members'),
    ('members:update',  'Change a member''s role'),
    ('members:remove',  'Remove a member from the team'),
    ('api_keys:read',   'List API keys'),
    ('api_keys:create', 'Create API keys'),
    ('api_keys:revoke', 'Revoke API keys'),
    ('jobs:read',       'View jobs'),
    ('jobs:write',      'Create/update jobs'),
    ('jobs:delete',     'Delete jobs'),
    ('executions:read', 'View job executions');

-- --------------------------------------------------------------------
-- Roles. System roles (team_id NULL) are shared templates - OWNER,
-- ADMIN, MEMBER - available to every team without being duplicated per
-- team. Custom roles (team_id NOT NULL) are team-scoped, e.g. a team
-- creating a "Developer" role with jobs:read + jobs:write +
-- executions:read. This is what makes permissions "configurable rather
-- than hard-coded forever" per team, while the three system roles stay
-- fixed reference points for the ownership/hierarchy rules below.
-- --------------------------------------------------------------------
CREATE TABLE roles (
    id          UUID PRIMARY KEY DEFAULT uuidv7(),
    team_id     UUID REFERENCES teams(id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    is_system   BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- System roles have no team; custom roles must be uniquely named
    -- within their team. Partial unique indexes below enforce both.
    CONSTRAINT system_role_has_no_team
        CHECK (NOT is_system OR team_id IS NULL)
);

CREATE UNIQUE INDEX idx_roles_system_name
    ON roles (name) WHERE is_system;
CREATE UNIQUE INDEX idx_roles_team_name
    ON roles (team_id, name) WHERE NOT is_system;

CREATE TABLE role_permissions (
    role_id         UUID NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
    permission_key  TEXT NOT NULL REFERENCES permissions(key) ON DELETE CASCADE,
    PRIMARY KEY (role_id, permission_key)
);

-- Seed the three system roles + their permission sets.
INSERT INTO roles (id, team_id, name, is_system) VALUES
    ('00000000-0000-0000-0000-000000000001', NULL, 'OWNER',  TRUE),
    ('00000000-0000-0000-0000-000000000002', NULL, 'ADMIN',  TRUE),
    ('00000000-0000-0000-0000-000000000003', NULL, 'MEMBER', TRUE);

INSERT INTO role_permissions (role_id, permission_key)
SELECT '00000000-0000-0000-0000-000000000001', key FROM permissions;
-- OWNER gets every permission in the catalog above, plus team-deletion
-- authority which is enforced in application code (see auth guard notes
-- in team.service.js) rather than as a catalog permission, since
-- "delete team" is a one-time destructive action tied to being the
-- sole owner, not an assignable permission.

INSERT INTO role_permissions (role_id, permission_key) VALUES
    ('00000000-0000-0000-0000-000000000002', 'members:invite'),
    ('00000000-0000-0000-0000-000000000002', 'members:remove'),
    ('00000000-0000-0000-0000-000000000002', 'members:update'),
    ('00000000-0000-0000-0000-000000000002', 'members:read'),
    ('00000000-0000-0000-0000-000000000002', 'team:read'),
    ('00000000-0000-0000-0000-000000000002', 'api_keys:read'),
    ('00000000-0000-0000-0000-000000000002', 'api_keys:create'),
    ('00000000-0000-0000-0000-000000000002', 'api_keys:revoke'),
    ('00000000-0000-0000-0000-000000000002', 'jobs:read'),
    ('00000000-0000-0000-0000-000000000002', 'jobs:write'),
    ('00000000-0000-0000-0000-000000000002', 'jobs:delete'),
    ('00000000-0000-0000-0000-000000000002', 'executions:read');

INSERT INTO role_permissions (role_id, permission_key) VALUES
    ('00000000-0000-0000-0000-000000000003', 'team:read'),
    ('00000000-0000-0000-0000-000000000003', 'members:read'),
    ('00000000-0000-0000-0000-000000000003', 'jobs:read'),
    ('00000000-0000-0000-0000-000000000003', 'jobs:write'),
    ('00000000-0000-0000-0000-000000000003', 'executions:read');
-- Note: jobs:delete is intentionally withheld from MEMBER by default -
-- "delete own/authorized jobs" needs a row-level owner check in the
-- jobs service (job.created_by = current user), not just a role grant.
-- That check is added when jobs gain a team_id/created_by column.

-- --------------------------------------------------------------------
-- Team memberships. This replaces users.account_id + users.role: a
-- user's relationship to a team - which team, which role - lives here,
-- one row per (team, user), so one user can appear in many rows with
-- a different role in each.
-- --------------------------------------------------------------------
CREATE TABLE team_memberships (
    id          UUID PRIMARY KEY DEFAULT uuidv7(),
    team_id     UUID NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    user_id     UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role_id     UUID NOT NULL REFERENCES roles(id),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (team_id, user_id)
);

CREATE INDEX idx_team_memberships_team_id ON team_memberships(team_id);
CREATE INDEX idx_team_memberships_user_id ON team_memberships(user_id);

-- Backfill: every existing user becomes a membership row in their
-- current account/team, mapped to the matching system role.
INSERT INTO team_memberships (team_id, user_id, role_id)
SELECT
    u.account_id,
    u.id,
    CASE u.role
        WHEN 'owner'  THEN '00000000-0000-0000-0000-000000000001'
        WHEN 'admin'  THEN '00000000-0000-0000-0000-000000000002'
        WHEN 'member' THEN '00000000-0000-0000-0000-000000000003'
    END
FROM users u;

ALTER TABLE users DROP COLUMN account_id;
ALTER TABLE users DROP COLUMN role;
