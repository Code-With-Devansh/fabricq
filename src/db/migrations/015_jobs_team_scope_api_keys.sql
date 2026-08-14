-- Phase 2: jobs belong to a team, and teams can issue scoped API keys
-- for the public API.

ALTER TABLE http_jobs
    ADD COLUMN team_id UUID NOT NULL REFERENCES teams(id) ON DELETE CASCADE;

CREATE INDEX idx_http_jobs_team ON http_jobs(team_id);
CREATE INDEX idx_http_jobs_team_status ON http_jobs(team_id, enabled);

-- job_executions intentionally does NOT get its own team_id - a job's
-- team never changes, so execution team-scoping is just a join through
-- http_jobs. Duplicating it here would be a denormalization with no
-- query this app actually needs yet.

-- --------------------------------------------------------------------
-- API keys. Machine authentication for the public API (/v1/*), separate
-- from the human JWT+role system. A key's scopes are checked against the
-- creating user's own team permissions at creation time (see
-- apiKey.service.js) so a key can never grant more than its creator has.
-- --------------------------------------------------------------------
CREATE TABLE api_keys (
    id              UUID PRIMARY KEY DEFAULT uuidv7(),
    team_id         UUID NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    key_prefix      TEXT NOT NULL,
    key_hash        TEXT NOT NULL UNIQUE,
    scopes          TEXT[] NOT NULL DEFAULT '{}',
    expires_at      TIMESTAMPTZ,
    revoked_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by      UUID REFERENCES users(id) ON DELETE SET NULL,
    last_used_at    TIMESTAMPTZ
);

CREATE INDEX idx_api_keys_team ON api_keys(team_id);
-- Lookup path for authenticateApiKey: find candidates by prefix first
-- (cheap, indexed), then verify the full hash - never scan by hash alone
-- since key_hash is effectively a secret comparison target.
CREATE INDEX idx_api_keys_prefix ON api_keys(key_prefix);
