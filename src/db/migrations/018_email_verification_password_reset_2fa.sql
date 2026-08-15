-- Phase 2.0: email verification, forgot password, and email-OTP 2FA.
--
-- All three share one shape: a short-lived secret is generated, only
-- its hash is persisted, and the raw value is emailed to the user
-- (link for verify/reset, 6-digit code for 2FA). This mirrors
-- refresh_tokens/team_invitations rather than introducing a new pattern.

ALTER TABLE users
    ADD COLUMN email_verified_at   TIMESTAMPTZ,
    ADD COLUMN two_factor_enabled  BOOLEAN NOT NULL DEFAULT FALSE;

-- --------------------------------------------------------------------
-- Email verification. Link-based (long-lived, ~24h) - clicked from an
-- inbox rather than typed in.
-- --------------------------------------------------------------------
CREATE TABLE email_verification_tokens (
    id              UUID PRIMARY KEY DEFAULT uuidv7(),
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash      TEXT NOT NULL UNIQUE,
    expires_at      TIMESTAMPTZ NOT NULL,
    consumed_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_email_verification_tokens_user_id
    ON email_verification_tokens(user_id);

-- --------------------------------------------------------------------
-- Forgot password. Link-based (short-lived, ~1h).
-- --------------------------------------------------------------------
CREATE TABLE password_reset_tokens (
    id              UUID PRIMARY KEY DEFAULT uuidv7(),
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash      TEXT NOT NULL UNIQUE,
    expires_at      TIMESTAMPTZ NOT NULL,
    consumed_at     TIMESTAMPTZ,
    requested_ip    TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_password_reset_tokens_user_id
    ON password_reset_tokens(user_id);

-- --------------------------------------------------------------------
-- Two-factor (email OTP). Code-based (short-lived, ~10 min), used both
-- for the login challenge and for confirming enablement. attempts
-- guards against brute-forcing a 6-digit code within its TTL.
-- --------------------------------------------------------------------
CREATE TABLE two_factor_challenges (
    id              UUID PRIMARY KEY DEFAULT uuidv7(),
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    code_hash       TEXT NOT NULL,
    purpose         TEXT NOT NULL CHECK (purpose IN ('login', 'enable')),
    attempts        INT NOT NULL DEFAULT 0,
    expires_at      TIMESTAMPTZ NOT NULL,
    consumed_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_two_factor_challenges_user_id
    ON two_factor_challenges(user_id);
