-- Indexes to support the retention job's batched deletes (src/retention).
-- Without these, every cleanup query below seq-scans its table - fine
-- today while these tables are small, but the entire point of adding
-- retention is that they won't stay small, and a seq-scanning DELETE on
-- a growing table is exactly the kind of thing that's cheap to add now
-- and painful to retrofit once the table is large and under load.
--
-- All five token/invite tables use expires_at as the sole staleness
-- anchor (see authCleanup.js for the reasoning) - once a row is past
-- expires_at by the retention window, it's unusable regardless of
-- whether it was ever consumed/revoked/accepted, so a single plain index
-- on expires_at is sufficient; no partial-index tricks needed since the
-- cleanup query always filters on it.

CREATE INDEX idx_refresh_tokens_expires_at ON refresh_tokens(expires_at);
CREATE INDEX idx_team_invitations_expires_at ON team_invitations(expires_at);
CREATE INDEX idx_email_verification_tokens_expires_at ON email_verification_tokens(expires_at);
CREATE INDEX idx_password_reset_tokens_expires_at ON password_reset_tokens(expires_at);
CREATE INDEX idx_two_factor_challenges_expires_at ON two_factor_challenges(expires_at);

-- execution_outbox already has a partial index for the "find unpublished
-- rows" scan (idx_execution_outbox_unpublished). This is the mirror image
-- for the retention job's "find published rows old enough to delete"
-- scan - without it, that query would have to seq-scan every published
-- row just to find the old ones, on a table whose whole problem was
-- unbounded growth of published rows in the first place.
CREATE INDEX idx_execution_outbox_published
ON execution_outbox(published_at)
WHERE published_at IS NOT NULL;
