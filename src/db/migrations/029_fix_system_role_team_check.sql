-- system_role_has_no_team was a one-way implication (is_system -> team_id
-- IS NULL), so it silently allowed an orphaned custom role: is_system =
-- false, team_id = NULL. That row also evades idx_roles_team_name (a
-- partial unique index on (team_id, name) WHERE NOT is_system), since
-- Postgres unique indexes treat NULL as distinct from NULL - so multiple
-- same-named orphaned custom roles could exist without colliding.
--
-- Replaced with a biconditional: system roles have no team, and non-system
-- roles always have one. No orphans in either direction.
ALTER TABLE roles
    DROP CONSTRAINT system_role_has_no_team,
    ADD CONSTRAINT system_role_has_no_team
        CHECK (is_system = (team_id IS NULL));
