-- Dashboard auth: accounts are the tenancy boundary, users belong to an
-- account. api_keys (phase 2) will also hang off account_id.

CREATE TABLE accounts (
    id          UUID PRIMARY KEY DEFAULT uuidv7(),
    name        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE EXTENSION IF NOT EXISTS citext;

CREATE TABLE users (
    id              UUID PRIMARY KEY DEFAULT uuidv7(),
    account_id      UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    email           CITEXT NOT NULL UNIQUE,
    password_hash   TEXT NOT NULL,
    role            TEXT NOT NULL DEFAULT 'owner'
                        CHECK (role IN ('owner', 'admin', 'member')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_users_account_id ON users(account_id);

-- Refresh tokens are opaque, random, single-use (rotated on refresh).
-- Only the hash is stored; the raw token is shown to the client once.
CREATE TABLE refresh_tokens (
    id              UUID PRIMARY KEY DEFAULT uuidv7(),
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash      TEXT NOT NULL UNIQUE,
    expires_at      TIMESTAMPTZ NOT NULL,
    revoked_at      TIMESTAMPTZ,
    replaced_by     UUID REFERENCES refresh_tokens(id),
    user_agent      TEXT,
    ip              TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_refresh_tokens_user_id ON refresh_tokens(user_id);
-- Fast lookup path for the (unexpired, unrevoked) refresh check.
CREATE INDEX idx_refresh_tokens_active
    ON refresh_tokens(user_id)
    WHERE revoked_at IS NULL;
