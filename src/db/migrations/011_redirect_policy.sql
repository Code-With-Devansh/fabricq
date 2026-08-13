ALTER TABLE http_jobs
    ADD COLUMN redirect_policy JSONB NOT NULL DEFAULT jsonb_build_object(
        'maxRedirects', 10,
        'allowCrossOrigin', false,
        'allowHttpDowngrade', false
    );

-- Guardrail so a bad/malicious value can't be used to make the worker
-- follow an unbounded redirect chain (resource abuse / SSRF pivoting).
ALTER TABLE http_jobs
    ADD CONSTRAINT valid_redirect_policy_max_redirects CHECK (
        (redirect_policy->>'maxRedirects')::int BETWEEN 0 AND 20
    );