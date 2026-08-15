-- Phase 1.9: team invitations.
--
-- Invites are opaque tokens (same pattern as refresh_tokens / api_keys:
-- only the hash is persisted, the raw token is shown to the invited
-- person once, in the accept link). An invitation targets an email, not
-- a user_id, since the invited person may not have an account yet.

CREATE TABLE team_invitations (
    id              UUID PRIMARY KEY DEFAULT uuidv7(),
    team_id         UUID NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    email           CITEXT NOT NULL,
    role_id         UUID NOT NULL REFERENCES roles(id),
    token_hash      TEXT NOT NULL UNIQUE,
    invited_by      UUID REFERENCES users(id) ON DELETE SET NULL,
    status          TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'accepted', 'revoked')),
    expires_at      TIMESTAMPTZ NOT NULL,
    accepted_at     TIMESTAMPTZ,
    accepted_by     UUID REFERENCES users(id) ON DELETE SET NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_team_invitations_team_id ON team_invitations(team_id);

-- One live invite per (team, email) at a time - re-inviting reuses/
-- replaces the pending row rather than stacking duplicates.
CREATE UNIQUE INDEX idx_team_invitations_pending_unique
    ON team_invitations (team_id, email) WHERE status = 'pending';
