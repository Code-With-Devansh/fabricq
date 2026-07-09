
CREATE TYPE body_type AS ENUM (
    'json',
    'form'
);

CREATE TYPE auth_type AS ENUM (
    'NONE',
    'BEARER',
    'BASIC',
    'API_KEY'
);

CREATE TYPE redirect_mode AS ENUM (
    'follow',
    'manual',
    'error'
);

ALTER TABLE http_jobs
    ADD COLUMN query_params JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN body_type body_type NOT NULL DEFAULT 'json',
    ADD COLUMN auth_type auth_type NOT NULL DEFAULT 'NONE',
    ADD COLUMN auth_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN redirect_mode redirect_mode NOT NULL DEFAULT 'follow',
    ADD COLUMN timeout_ms INTEGER NOT NULL DEFAULT 30000,
    ADD CONSTRAINT valid_timeout CHECK (timeout_ms > 0 AND timeout_ms <= 120000);
